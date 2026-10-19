import pytest
from valkey import ResponseError, Valkey, ValkeyCluster

from valkey_timeseries_test_case import ValkeyTimeSeriesClusterTestCase
from valkeytestframework.conftest import resource_port_tracker
from common import LabelSearchResponse, parse_stats_response


# Keys placed deterministically on distinct shards of a 3-primary cluster.
# Slots are split evenly at [0, 5461), [5461, 10922), [10922, 16384), so the
# hash tags below land one key on each shard:
#   {3} -> slot 1584   -> shard 0
#   {1} -> slot 9842   -> shard 1
#   {4} -> slot 14039  -> shard 2
# This lets us exercise the fanout path where the coordinator is NOT the node
# that hosts a given key, which is exactly where cross-cluster identity
# propagation matters.
KEY_SHARD0 = b"ts:acl:{3}:cpu"
KEY_SHARD1 = b"ts:acl:{1}:cpu"
KEY_SHARD2 = b"ts:acl:{4}:cpu"
ALL_KEYS = [KEY_SHARD0, KEY_SHARD1, KEY_SHARD2]

# ACL key patterns granting read access to exactly one of the keys above.
# Note: `{` / `}` are literal characters in Valkey ACL glob patterns.
PATTERN_SHARD0 = "~ts:acl:{3}:*"
PATTERN_SHARD1 = "~ts:acl:{1}:*"
PATTERN_SHARD2 = "~ts:acl:{4}:*"


class TestTimeSeriesAclCluster(ValkeyTimeSeriesClusterTestCase):
    """Cross-cluster ACL tests for the fanout (multi-shard) commands.

    Two properties are under test:

    1. **Identity preservation** - the user that issues a multi-shard command on
       the coordinator is the identity every shard enforces ACLs against, no
       matter which node coordinates the fanout.

    2. **Shard-side enforcement** - each fanout command applies the documented
       ACL policy on every shard:
         * TS.QUERYINDEX / TS.CARD without range filters -> index lookups, reveal
           matches regardless of the caller's per-key read access.
         * TS.CARD with FILTER_BY_RANGE -> fail closed on unreadable matches.
         * TS.MGET / TS.MRANGE / TS.MREVRANGE -> data-returning, fail closed if
           the filter matches any key the caller cannot read.
         * TS.MDEL -> destructive, fails closed if the filter matches any key the
           caller cannot delete (unauthorized keys are never removed).
         * TS.LABELVALUES / TS.LABELNAMES -> metadata, require all-keys read.
    """

    # ------------------------------------------------------------------ helpers

    def _setup_per_shard_data(self, cluster: ValkeyCluster):
        """Create one series per shard, all sharing `name=cpu`.

        Each series also gets a distinct `host` value and a per-shard unique
        label name, so a single `FILTER name=cpu` matches all three keys while
        narrower filters can target an individual (authorized) key.
        """
        specs = [
            (KEY_SHARD0, "h0", "only0"),
            (KEY_SHARD1, "h1", "only1"),
            (KEY_SHARD2, "h2", "only2"),
        ]
        for key, host, uniq in specs:
            cluster.execute_command(
                "TS.CREATE", key, "LABELS", "name", "cpu", "host", host, uniq, "y"
            )
            cluster.execute_command("TS.ADD", key, 1000, 10)
            cluster.execute_command("TS.ADD", key, 2000, 20)

    def _create_user_on_all_primaries(self, username: str, password: str, *rules: str):
        # The user must exist with identical rules on every shard, because each
        # shard authenticates the propagated identity and checks ACLs locally.
        for index in range(self.CLUSTER_SIZE):
            primary = self.client_for_primary(index)
            primary.execute_command(
                "ACL", "SETUSER", username, "RESET", "ON", f">{password}", *rules
            )

    def _get_primary_user_client(self, primary_index: int, username: str, password: str) -> Valkey:
        client = self.new_client_for_primary(primary_index)
        client.execute_command("AUTH", username, password)
        return client

    @staticmethod
    def _mget_keys(client, *filters):
        rows = client.execute_command("TS.MGET", "FILTER", *filters)
        return sorted(row[0] for row in rows)

    # ============================================================ identity ====

    def test_full_access_user_sees_all_keys_from_any_coordinator(self):
        """A `~*` user gets the full result set no matter which node coordinates.

        Every coordinator fans out to all shards; each shard authorizes the
        propagated `alice` identity, so the aggregated reply is identical
        regardless of entry point.
        """
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("alice", "pw", "+@timeseries", "+@read", "~*")

        for coordinator in range(self.CLUSTER_SIZE):
            alice = self._get_primary_user_client(coordinator, "alice", "pw")
            keys = self._mget_keys(alice, "name=cpu")
            assert keys == sorted(ALL_KEYS), f"coordinator={coordinator} returned {keys}"

    def test_restricted_user_mget_fails_closed_from_any_coordinator(self):
        """A restricted user's identity is enforced on remote shards too.

        `bob` may only read the shard-1 key, but `FILTER name=cpu` also matches
        the shard-0 and shard-2 keys. TS.MGET must fail closed from *every*
        coordinator - including coordinators 0 and 2, whose local shard does not
        even host the key bob is allowed to read - proving the denial is applied
        remotely against bob's identity (not the connecting node's default user).
        """
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("bob", "pw", "+@timeseries", "+@read", PATTERN_SHARD1)

        for coordinator in range(self.CLUSTER_SIZE):
            bob = self._get_primary_user_client(coordinator, "bob", "pw")
            with pytest.raises(ResponseError) as exc:
                bob.execute_command("TS.MGET", "FILTER", "name=cpu")
            assert "permission" in str(exc.value).lower(), (
                f"coordinator={coordinator} raised {exc.value!r}"
            )

    def test_identity_grants_access_to_authorized_remote_key(self):
        """Identity travels to a remote shard and grants access there.

        `carol` may only read the shard-2 key. Coordinating from shard 0 (which
        does not host that key) with a filter that matches ONLY the authorized
        key returns the data - the grant is applied on the remote shard using
        the propagated identity.
        """
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("carol", "pw", "+@timeseries", "+@read", PATTERN_SHARD2)

        carol = self._get_primary_user_client(0, "carol", "pw")
        keys = self._mget_keys(carol, "host=h2")
        assert keys == [KEY_SHARD2]

    def test_identity_isolation_between_users_on_same_coordinator(self):
        """Interleaved requests from two users through one coordinator stay isolated.

        Each request must be evaluated against its own caller's identity; a stale
        or shared thread-local user would let one user see the other's key.
        """
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("alice", "pw", "+@timeseries", "+@read", PATTERN_SHARD1)
        self._create_user_on_all_primaries("bob", "pw", "+@timeseries", "+@read", PATTERN_SHARD2)

        alice = self._get_primary_user_client(0, "alice", "pw")
        bob = self._get_primary_user_client(0, "bob", "pw")

        for _ in range(3):
            assert self._mget_keys(alice, "host=h1") == [KEY_SHARD1]
            assert self._mget_keys(bob, "host=h2") == [KEY_SHARD2]

    def test_missing_user_on_one_shard_fails_closed(self):
        """If the propagated identity does not exist on a shard, the command fails.

        `dave` has full read access but is deleted on shard 1. The shard cannot
        authenticate the propagated identity, so the fanout fails closed instead
        of silently skipping that shard.
        """
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("dave", "pw", "+@timeseries", "+@read", "~*")

        # Simulate an ACL mismatch across the cluster.
        self.client_for_primary(1).execute_command("ACL", "DELUSER", "dave")

        dave = self._get_primary_user_client(0, "dave", "pw")
        with pytest.raises(ResponseError):
            dave.execute_command("TS.MGET", "FILTER", "name=cpu")

    # ==================================================== per-command ACL ====

    def test_default_admin_user_sees_all_keys(self):
        """The unrestricted default user is unaffected: it sees every shard's keys."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)

        admin = self.new_client_for_primary(0)
        assert self._mget_keys(admin, "name=cpu") == sorted(ALL_KEYS)

    def test_queryindex_reveals_matching_keys_regardless_of_read_acl(self):
        """TS.QUERYINDEX is an index lookup: it reveals matches even when the
        caller cannot read some of them."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("q", "pw", "+@timeseries", "+@read", PATTERN_SHARD1)

        q = self._get_primary_user_client(0, "q", "pw")
        keys = sorted(q.execute_command("TS.QUERYINDEX", "name=cpu"))
        assert keys == sorted(ALL_KEYS)

    def test_card_reveals_full_count_regardless_of_read_acl(self):
        """TS.CARD counts index matches across all shards regardless of per-key
        read access."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("q", "pw", "+@timeseries", "+@read", PATTERN_SHARD1)

        q = self._get_primary_user_client(0, "q", "pw")
        assert int(q.execute_command("TS.CARD", "FILTER", "name=cpu")) == 3

    def test_queryindex_preserves_acl_policy_across_batches(self):
        cluster = self.new_cluster_client()
        pipe = cluster.pipeline(transaction=False)
        allowed = [f'qi:allowed:{{3}}:{i}'.encode() for i in range(1024)]
        denied = b'qi:denied:{3}:last'
        for key in allowed:
            pipe.execute_command(
                'TS.ADD', key, 1000, 1, 'LABELS', 'fleet', 'queryindex-acl', 'access', 'allowed',
            )
        pipe.execute_command(
            'TS.ADD', denied, 5000, 1, 'LABELS', 'fleet', 'queryindex-acl', 'access', 'denied',
        )
        pipe.execute()
        self._create_user_on_all_primaries('qi-reader', 'pw', '+ts.queryindex', '~qi:allowed:*')

        for coordinator in range(self.CLUSTER_SIZE):
            reader = self._get_primary_user_client(coordinator, 'qi-reader', 'pw')
            assert reader.execute_command('TS.QUERYINDEX', 'fleet=queryindex-acl') == sorted(allowed + [denied])
            assert reader.execute_command(
                'TS.QUERYINDEX', 'FILTER_BY_RANGE', 1000, 1000,
                'fleet=queryindex-acl', 'access=allowed',
            ) == sorted(allowed)
            for modifier in [(), ('NOT',)]:
                with pytest.raises(ResponseError, match='permission'):
                    reader.execute_command(
                        'TS.QUERYINDEX', 'FILTER_BY_RANGE', *modifier, 1000, 1000,
                        'fleet=queryindex-acl',
                    )

    def test_card_date_range_enforces_acl_after_first_batch(self):
        """Range counts check access before the date predicate, including in later batches."""
        cluster = self.new_cluster_client()
        pipe = cluster.pipeline(transaction=False)
        # One shard holds two full batches the reader may access, followed by a denied key.
        for i in range(1024):
            pipe.execute_command(
                'TS.ADD', f'card:allowed:{{3}}:{i}', 1000, i,
                'LABELS', 'fleet', 'card-acl', 'access', 'allowed',
            )
        pipe.execute_command(
            'TS.ADD', 'card:denied:{3}:last', 5000, 1,
            'LABELS', 'fleet', 'card-acl', 'access', 'denied',
        )
        pipe.execute()
        self._create_user_on_all_primaries(
            'card-reader', 'pw', '+ts.card', '~card:allowed:*',
        )

        for coordinator in range(self.CLUSTER_SIZE):
            reader = self._get_primary_user_client(coordinator, 'card-reader', 'pw')
            # The index-only count still reveals every match.
            assert reader.execute_command('TS.CARD', 'FILTER', 'fleet=card-acl') == 1025
            assert reader.execute_command(
                'TS.CARD', 'FILTER_BY_RANGE', 1000, 1000,
                'FILTER', 'fleet=card-acl', 'access=allowed',
            ) == 1024
            for modifier in [(), ('NOT',)]:
                with pytest.raises(ResponseError, match='permission'):
                    reader.execute_command(
                        'TS.CARD', 'FILTER_BY_RANGE', *modifier, 1000, 1000,
                        'FILTER', 'fleet=card-acl',
                    )

    def test_mget_returns_data_when_all_matched_keys_authorized(self):
        """TS.MGET succeeds when the filter matches only keys the caller can read,
        even if the sole match lives on a remote shard."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("reader", "pw", "+@timeseries", "+@read", PATTERN_SHARD1)

        reader = self._get_primary_user_client(0, "reader", "pw")
        assert self._mget_keys(reader, "host=h1") == [KEY_SHARD1]

    def test_mrange_fails_closed_when_matching_unauthorized_keys(self):
        """TS.MRANGE returns data, so it must fail closed on an unauthorized match."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("reader", "pw", "+@timeseries", "+@read", PATTERN_SHARD1)

        reader = self._get_primary_user_client(0, "reader", "pw")
        with pytest.raises(ResponseError) as exc:
            reader.execute_command("TS.MRANGE", 0, 10000, "FILTER", "name=cpu")
        assert "permission" in str(exc.value).lower()

    def test_mrevrange_fails_closed_when_matching_unauthorized_keys(self):
        """TS.MREVRANGE shares the MRANGE fanout and must also fail closed."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("reader", "pw", "+@timeseries", "+@read", PATTERN_SHARD1)

        reader = self._get_primary_user_client(0, "reader", "pw")
        with pytest.raises(ResponseError) as exc:
            reader.execute_command("TS.MREVRANGE", 0, 10000, "FILTER", "name=cpu")
        assert "permission" in str(exc.value).lower()

    def test_mrange_returns_data_when_all_matched_keys_authorized(self):
        """TS.MRANGE succeeds and returns samples when every match is authorized."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries("reader", "pw", "+@timeseries", "+@read", PATTERN_SHARD1)

        reader = self._get_primary_user_client(0, "reader", "pw")
        rows = reader.execute_command("TS.MRANGE", 0, 10000, "FILTER", "host=h1")
        keys = sorted(row[0] for row in rows)
        assert keys == [KEY_SHARD1]
        # samples for the authorized key are returned
        assert len(rows[0][2]) == 2

    def test_mdel_fails_closed_when_matching_unauthorized_keys(self):
        """TS.MDEL is destructive: it fails closed and never deletes a key the
        caller lacks delete permission on."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries(
            "carol", "pw", "+@timeseries", "+@read", "+@write", PATTERN_SHARD1
        )

        carol = self._get_primary_user_client(0, "carol", "pw")
        with pytest.raises(ResponseError):
            carol.execute_command("TS.MDEL", "FILTER", "name=cpu")

        # The keys carol may not delete (on shards that rejected the request)
        # must still exist.
        assert cluster.execute_command("EXISTS", KEY_SHARD0) == 1
        assert cluster.execute_command("EXISTS", KEY_SHARD2) == 1

    def test_labelstats_checks_metadata_permissions_on_every_shard(self):
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries('stats-reader', 'pw', '+ts.labelstats', '~*')
        # The coordinators on shards 0 and 2 admit the request; shard 1 must reject its share.
        self.client_for_primary(1).execute_command(
            'ACL', 'SETUSER', 'stats-reader', 'RESETKEYS', PATTERN_SHARD1,
        )
        for coordinator in range(self.CLUSTER_SIZE):
            reader = self._get_primary_user_client(coordinator, 'stats-reader', 'pw')
            try:
                with pytest.raises(ResponseError, match='permission'):
                    reader.execute_command('TS.LABELSTATS', 'FILTER', 'name=cpu')
            finally:
                reader.close()
        self.client_for_primary(1).execute_command('ACL', 'SETUSER', 'stats-reader', '~*')
        reader = self._get_primary_user_client(0, 'stats-reader', 'pw')
        try:
            assert parse_stats_response(reader.execute_command('TS.LABELSTATS'))['totalSeries'] == 3
        finally:
            reader.close()

    def test_labelvalues_requires_all_keys_read_permission(self):
        """TS.LABELVALUES exposes metadata that could leak across keys, so it
        requires all-keys read access. A restricted user is denied; a `~*` user
        gets values aggregated from every shard."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries(
            "restricted", "pw", "+@timeseries", "+@read", PATTERN_SHARD1
        )
        self._create_user_on_all_primaries("full", "pw", "+@timeseries", "+@read", "~*")

        restricted = self._get_primary_user_client(0, "restricted", "pw")
        with pytest.raises(ResponseError):
            restricted.execute_command("TS.LABELVALUES", "host", "FILTER", "name=cpu")

        full = self._get_primary_user_client(0, "full", "pw")
        raw = full.execute_command("TS.LABELVALUES", "host", "FILTER", "name=cpu")
        values = sorted(lv.value for lv in LabelSearchResponse.parse(raw).results)
        assert values == [b"h0", b"h1", b"h2"]

    def test_labelnames_requires_all_keys_read_permission(self):
        """TS.LABELNAMES is metadata too: a restricted user is denied, while a
        `~*` user sees label names contributed by series on every shard."""
        cluster = self.new_cluster_client()
        self._setup_per_shard_data(cluster)
        self._create_user_on_all_primaries(
            "restricted", "pw", "+@timeseries", "+@read", PATTERN_SHARD1
        )
        self._create_user_on_all_primaries("full", "pw", "+@timeseries", "+@read", "~*")

        restricted = self._get_primary_user_client(0, "restricted", "pw")
        with pytest.raises(ResponseError):
            restricted.execute_command("TS.LABELNAMES", "FILTER", "name=cpu")

        full = self._get_primary_user_client(0, "full", "pw")
        raw = full.execute_command("TS.LABELNAMES", "FILTER", "name=cpu")
        names = {lv.value for lv in LabelSearchResponse.parse(raw).results}
        # Per-shard unique label names must all be present for the full-access user.
        assert {b"only0", b"only1", b"only2"}.issubset(names)

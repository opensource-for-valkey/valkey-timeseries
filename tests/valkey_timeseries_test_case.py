import os
import shutil
import pytest
from valkeytestframework.util.waiters import wait_for_equal, TEST_MAX_WAIT_TIME_SECONDS
from valkeytestframework.valkey_test_case import ValkeyTestCase
from valkeytestframework.valkey_test_case import ReplicationTestCase
from valkeytestframework.valkey_test_case import ValkeyServerHandle
from valkeytestframework.util import waiters
from valkey import ResponseError
from valkey.cluster import ValkeyCluster, ClusterNode
from valkey.client import Valkey
from valkey.connection import Connection
from typing import List, Tuple
from common import VALKEY_SERVER_PATH, LOGS_DIR, ValkeyInfo, CompactionRule, parse_info_response, TEST_DIR, \
    MODULE_PATH as DEFAULT_MODULE_PATH, get_module_path
import random
import string
import logging


class Node:
    """This class represents a valkey server instance, regardless of its role"""

    def __init__(
            self,
            client=None,
            server=None,
            logfile=None,
    ):
        self.client: Valkey = client
        self.server: ValkeyServerHandle = server
        self.logfile: str = logfile

    def does_logfile_contains(self, pattern: str) -> bool:
        try:
            logfile = open(self.logfile, "r")
            for line in logfile:
                if pattern in line:
                    return True
            return False
        except:
            return False


class ReplicationGroup:
    """This class represents a valkey server replication group. A replication group consists of a single primary node
    and 0 or more replication nodes."""

    def __init__(
            self,
            primary,
            replicas,
    ):
        self.primary: Node = primary
        self.replicas: List[Node] = replicas

    def setup_replications_cluster(self):
        node_id: bytes = self.primary.client.cluster("MYID")
        node_id = node_id.decode("utf-8")
        for replica in self.replicas:
            replica.client.execute_command(f"CLUSTER REPLICATE {node_id}")
        self._wait_for_replication()

    def setup_replications_cmd(self):
        primary_ip = self.primary.server.bind_ip
        primary_port = self.primary.server.port
        for replica in self.replicas:
            replica.client.execute_command(
                f"REPLICAOF {primary_ip} {primary_port}"
            )
        self._wait_for_replication()

    def _wait_for_replication(self):
        # connected_slaves:1
        # slave0:ip=127.0.0.1,port=14892,state=online,offset=98,lag=0,type=replica
        waiters.wait_for_true(
            lambda: self._check_all_replicas_are_connected(),
            timeout=30,
        )

        # Now that we see all the replicas are connected, wait for their status to change
        # into "online"
        for i in range(0, len(self.replicas)):
            name = f"slave{i}"
            # Wait for the replication to complete for this replica
            waiters.wait_for_true(
                lambda: self._check_is_replica_online(name),
                timeout=30,
            )

    def _check_all_replicas_are_connected(self):
        return self.primary.client.info("replication")[
            "connected_slaves"
        ] == len(self.replicas)

    def _check_is_replica_online(self, name) -> bool:
        replica_status = self.primary.client.info("replication")[name]
        return replica_status["state"] == "online"

    def _wait_for_meet(self, count: int) -> bool:
        d: dict = self.primary.client.cluster("NODES")
        if len(d) != count:
            return False
        for record in d.values():
            if not record["connected"]:
                return False
        return True

    def get_replica_connection(self, index) -> Valkey:
        return self.replicas[index].client

    def get_primary_connection(self) -> Valkey:
        return self.primary.client

    def get_num_keys(self, db=0):
        client = self.get_primary_connection()
        return client.num_keys(db)

    def verify_server_key_count(self, expected_num_keys):
        actual_num_keys = self.get_num_keys()
        assert_num_key_error_msg = f"Actual key number {actual_num_keys} is different from expected key number {expected_num_keys}"
        assert actual_num_keys == expected_num_keys, assert_num_key_error_msg

    def get_primary_repl_offset(self):
        primary = self.get_primary_connection()
        return primary.info()["master_repl_offset"]

    def get_replica_repl_offset(self, index):
        replica_client = self.get_replica_connection(index)
        return replica_client.info()["slave_repl_offset"]

    def get_primary_ts_info(self, key, debug = False):
        """ Get the info of the given key.
        """
        debug_str = 'DEBUG' if debug else ''
        client = self.get_primary_connection()
        info = client.execute_command(f'TS.INFO {key} {debug_str}')
        info_dict = parse_info_response(info)

        return info_dict

    def wait_for_replica_offset_to_sync_up(self, index):
        # Snapshot the primary's current replication offset, then wait for the
        # replica to reach that value.  The previous implementation had the
        # lambda and the expected value swapped: it polled the primary's
        # *current* (ever-increasing) offset and compared it against the
        # replica's offset captured once at call-time — a condition that can
        # never be satisfied once the primary advances past that snapshot.
        target = self.get_primary_repl_offset()
        wait_for_equal(
            lambda: self.get_replica_repl_offset(index),
            target,
            timeout=TEST_MAX_WAIT_TIME_SECONDS,
        )

    @staticmethod
    def cleanup(rg):
        """Kill all the replication group nodes"""
        if not rg:
            return

        def kill_node(node):
            if not node or not node.server or not getattr(node.server, "server", None):
                return
            try:
                os.kill(node.server.pid(), 9)
            except (AttributeError, ProcessLookupError):
                pass

        kill_node(rg.primary)

        if not rg.replicas:
            return

        for replica in rg.replicas:
            kill_node(replica)

class ValkeyTimeSeriesTestCaseCommon(ValkeyTestCase):
    """Common base class for the various Search test cases"""

    def resolve_module_path(self) -> str:
        """Resolve and validate the module path used by test servers."""
        module_path = os.path.abspath(os.getenv('MODULE_PATH') or get_module_path() or DEFAULT_MODULE_PATH)
        if not os.path.exists(module_path):
            raise FileNotFoundError(f"Module path does not exist: {module_path}")
        return module_path

    def normalize_dir_name(self, name: str) -> str:
        """Replace special chars from a string with an underscore"""
        chars_to_replace: str = "!@#$%^&*() -~[]{}><+"
        for char in chars_to_replace:
            name = name.replace(char, "_")
        return name


    def get_config_file_lines(self, testdir, port) -> List[str]:
        """A template method, must be implemented by subclasses
        See ValkeySearchTestCaseBase.get_config_file_lines & ValkeySearchClusterTestCase.get_config_file_lines
        for example usage."""
        raise NotImplementedError

    def verify_replicaof_succeeded(self, replica_client) -> None:
        info = replica_client.execute_command("INFO", "replication")
        role = info.get("role", "")
        master_link_status = info.get("master_link_status", "")
        assert role == "slave" and master_link_status == "up"

    def num_keys(self, db=0, client=None):
        dbKey = f"db{db}"
        if client is None:
            client = self.client
        allKeys = client.info("all").keys()
        if dbKey in allKeys:
            return client.info("all")["db{}".format(db)]["keys"]
        return 0

    def generate_random_string(self, length=7):
        """ Creates a random string with a specified length.
        """
        characters = string.ascii_letters + string.digits
        random_string = ''.join(random.choice(characters) for _ in range(length))
        return random_string

    def ts_info(self, key, debug = False):
        """ Get the info of the given key.
        """
        debug_str = 'DEBUG' if debug else ''
        info = self.client.execute_command(f'TS.INFO {key} {debug_str}')
        info_dict = parse_info_response(info)

        return info_dict
    
    def validate_ts_info_values(self, key, expected_info_dict):
        """ Validate the values of the timeseries info.
        """
        info_dict = self.ts_info(key)
        for k, v in expected_info_dict.items():
            if k == 'labels':
                assert info_dict[k] == v
            else:
                assert info_dict[k] == v, f"Expected {k} to be {v}, but got {info_dict[k]}"

    # A filter list must contain at least one matcher that cannot be satisfied by a missing
    # label, so that a query never degenerates into a full keyspace scan. Filters are
    # conjunctive, so the requirement applies to the list as a whole: one bounded filter
    # licenses any number of negative or empty-matching ones alongside it.
    UNBOUNDED_FILTER_ERROR = "at least one matcher that does not match the empty string"

    def assert_filters_rejected(self, *args, client=None):
        """Assert a command is rejected because its filter list lacks a bounded matcher."""
        client = client if client is not None else self.client
        with pytest.raises(ResponseError, match=self.UNBOUNDED_FILTER_ERROR):
            client.execute_command(*args)

    def verify_error_response(self, client, cmd, expected_err_reply):
        try:
            client.execute_command(cmd)
            assert False
        except ResponseError as e:
            assert_error_msg = f"Actual error message: '{str(e)}' is different from expected error message '{expected_err_reply}'"
            assert str(e) == expected_err_reply, assert_error_msg
            return str(e)

    def verify_command_success_reply(self, client, cmd, expected_result):
        cmd_actual_result = client.execute_command(cmd)
        assert_error_msg = f"Actual command response '{cmd_actual_result}' is different from expected response '{expected_result}'"
        assert cmd_actual_result == expected_result, assert_error_msg


    def verify_key_exists(self, client, key, value, should_exist=True):
        if should_exist:
            assert client.execute_command(f'EXISTS {key}') == 1, f"Item {key} {value} doesn't exist"
        else:
            assert client.execute_command(f'EXISTS {key}') == 0, f"Item {key} {value} exists"

    def verify_server_key_count(self, client, expected_num_keys):
        actual_num_keys = self.server.num_keys()
        assert_num_key_error_msg = f"Actual key number {actual_num_keys} is different from expected key number {expected_num_keys}"
        assert actual_num_keys == expected_num_keys, assert_num_key_error_msg

    """
    This method will parse the return of an INFO command and return a python dict where each metric is a key value pair.
    We can pass in specific sections in order to not have the dict store irrelevant fields related to what we want to check.
    Example of parsing the returned dict:
        stats = self.parse_valkey_info("STATS")
        stats.get('active_defrag_misses')
    """
    def valkey_info(self, section="all"):
        mem_info = self.client.execute_command('INFO ' + section)
        lines = mem_info.decode('utf-8').split('\r\n')
        stats_dict = {}
        for line in lines:
            if ':' in line:
                key, value = line.split(':', 1)
                stats_dict[key.strip()] = value.strip()
        return ValkeyInfo(stats_dict)

    def validate_copied_series_correctness(self, client, original_name):
        """ Validate correctness on a copy of the provided timeseries.
        """
        copy_filter_name = f"{original_name}_copy"
        assert client.execute_command(f'COPY {original_name} {copy_filter_name}') == 1
        assert client.execute_command('DBSIZE') == 2
        original_info_dict = self.ts_info(original_name)
        copy_info_dict = self.ts_info(copy_filter_name)

        assert copy_info_dict == original_info_dict, f"Expected {copy_info_dict} to be equal to {original_info_dict}"



class ValkeyTimeSeriesTestCaseBase(ValkeyTimeSeriesTestCaseCommon):

    # Extra module load arguments, as bare "name value" pairs appended to the loadmodule line.
    # Module configs are addressed by their unprefixed name at load time (the "ts." prefix
    # applies only to CONFIG GET/SET), e.g. "debug-mode yes".
    MODULE_ARGS: str = ""

    @pytest.fixture(autouse=True)
    def setup_test(self, request):
        module_path = self.resolve_module_path()
        loadmodule = f"{module_path} {self.MODULE_ARGS}".strip()
        args = {"enable-debug-command": "yes", 'loadmodule': loadmodule}
        server_path = VALKEY_SERVER_PATH

        self.server, self.client = self.create_server(testdir=self.testdir, server_path=server_path, args=args)
        logging.info("startup args are: %s", args)

    def validate_rules(self, key, expected_rules: List[CompactionRule], check_dest: bool = True):
        """ Validate the compaction rules of the timeseries.
        """
        info_dict = self.ts_info(key)
        if 'rules' not in info_dict:
            assert len(expected_rules) == 0, f"Expected no rules, but got {len(expected_rules)} rules: {expected_rules}"
            return

        actual_rules = info_dict['rules']
        assert len(actual_rules) == len(expected_rules), f"Expected {len(expected_rules)} rules, but got {len(actual_rules)}"

        # Convert actual rules to a set for easy comparison
        actual_rule_set = set()
        for rule in actual_rules:
            if isinstance(rule, CompactionRule):
                actual_rule_set.add(rule)
            else:
                raise TypeError(f"Unexpected type for actual rule: {type(rule)}")

        # Convert expected rules to a set
        expected_rule_set = set()
        for rule in expected_rules:
            if isinstance(rule, CompactionRule):
                expected_rule_set.add(rule)
            else:
                raise TypeError(f"Unexpected type for expected rule: {type(rule)}")

        # Find rules in the series but not in the expected list
        extra_rules = actual_rule_set - expected_rule_set
        if extra_rules:
            assert False, f"Found unexpected rules in series: {extra_rules}"

        # Find rules in the expected list but not in the series
        missing_rules = expected_rule_set - actual_rule_set
        if missing_rules:
            missing_rules_formatted = [rule in missing_rules]
            assert False, f"Expected rules not found in series: {missing_rules_formatted}"

        # If we get here, all rules match exactly
        if check_dest:
            for rule in expected_rules:
                if not isinstance(rule, CompactionRule):
                    raise TypeError(f"Expected rule to be of type CompactionRule, but got {type(rule)}")
                dest_key = rule.dest_key
                exists = self.client.execute_command("EXISTS", dest_key)
                assert exists == True, f"Expected destination key '{dest_key}' to exist, but it does not."


class ValkeyTimeSeriesClusterTestCase(ValkeyTimeSeriesTestCaseCommon):
    # Default cluster size
    CLUSTER_SIZE = 3
    # Default value for replication
    REPLICAS_COUNT = 0

    def _split_range_pairs(self, start, end, n):
        points = [start + i * (end - start) // n for i in range(n + 1)]
        return list(zip(points[:-1], points[1:]))

    def add_slots(self, node_idx, first_slot, last_slot):
        client: Valkey = self.client_for_primary(node_idx)
        slots_to_add = list(range(int(first_slot), int(last_slot)))
        client.execute_command("CLUSTER ADDSLOTS", *slots_to_add)

    def cluster_meet(self, node_idx, primaries_count):
        """We basically call meet for each node on all nodes in the cluster"""
        client: Valkey = self.client_for_primary(node_idx)
        current_node = self.replication_groups[node_idx]

        for node_to_meet in range(0, primaries_count):
            rg: ReplicationGroup = self.replication_groups[node_to_meet]

            # Prepare a list of instances (the primary + replicas if any)
            instances_arr: List[Node] = [rg.primary]
            for repl in rg.replicas:
                instances_arr.append(repl)

            for repl in current_node.replicas:
                instances_arr.append(repl)

            for inst in instances_arr:
                client.execute_command(
                    " ".join(
                        [
                            "CLUSTER",
                            "MEET",
                            f"{inst.server.bind_ip}",
                            f"{inst.server.port}",
                        ]
                    )
                )

    def start_server(
        self,
        port: int,
        test_name: str,
        module_path: str,
        cluster_enabled=True,
        is_primary=True,
    ) -> Tuple[ValkeyServerHandle, Valkey, str]:
        """Launch the server node and return a tuple of the server handle, a client to the server
        and the log file path"""
        server_path = VALKEY_SERVER_PATH
        testdir = f"{TEST_DIR}/{test_name}"

        os.makedirs(testdir, exist_ok=True)
        curdir = os.getcwd()
        os.chdir(testdir)
        lines = self.get_config_file_lines(testdir, port)

        conf_file = f"{testdir}/valkey_{port}.conf"
        with open(conf_file, "w+") as f:
            for line in lines:
                f.write(f"{line}\n")
            f.write("\n")
            f.close()

        role = "primary" if is_primary else "replica"
        logfile = f"{testdir}/logfile-{role}-{port}.log"
        server, client = self.create_server(
            testdir=testdir,
            server_path=server_path,
            args={"logfile": logfile},
            port=port,
            conf_file=conf_file,
        )
        os.chdir(curdir)
        self.wait_for_logfile(logfile, "Ready to accept connections")
        # Verify each node loaded the exact module artifact requested for this test run.
        self.wait_for_logfile(logfile, f"Module 'ts' loaded from {module_path}")
        client.ping()
        return server, client, logfile

    @pytest.fixture(autouse=True)
    def setup_test(self, request):
        module_path = self.resolve_module_path()
        replica_count = self.REPLICAS_COUNT
        # Extract parameters from pytest's parametrize decorator
        if hasattr(request.node, 'callspec') and 'setup_test' in request.node.callspec.params:
            param_value = request.node.callspec.params['setup_test']
            if isinstance(param_value, dict) and 'replica_count' in param_value:
                replica_count = param_value['replica_count']

        self.replication_groups: list[ReplicationGroup] = list()
        ports = []
        for i in range(0, self.CLUSTER_SIZE):
            ports.append(self.get_bind_port())
            for _ in range(0, replica_count):
                ports.append(self.get_bind_port())

        test_name = self.normalize_dir_name(request.node.name)
        testdir_base = f"{TEST_DIR}/{test_name}"

        if os.path.exists(testdir_base):
            shutil.rmtree(testdir_base)

        for i in range(0, len(ports), replica_count + 1):
            primary_port = ports[i]
            server, client, logfile = self.start_server(
                primary_port,
                test_name,
                module_path,
                True,
                is_primary=True,
            )

            replicas = []
            for _ in range(0, replica_count):
                # Start the replicas
                i = i + 1
                replica_port = ports[i]
                replica_server, replica_client, replica_logfile = (
                    self.start_server(
                        replica_port,
                        test_name,
                        module_path,
                        cluster_enabled=True,
                        is_primary=False,
                    )
                )
                replicas.append(
                    Node(
                        server=replica_server,
                        client=replica_client,
                        logfile=replica_logfile,
                    )
                )

            primary_node = Node(
                server=server,
                client=client,
                logfile=logfile,
            )
            rg = ReplicationGroup(primary=primary_node, replicas=replicas)
            self.replication_groups.append(rg)

        self.nodes: List[Node] = list()
        for rg in self.replication_groups:
            self.nodes.append(rg.primary)
            self.nodes += rg.replicas

        # Split the slots
        ranges = self._split_range_pairs(0, 16384, self.CLUSTER_SIZE)
        node_idx = 0
        for start, end in ranges:
            self.add_slots(node_idx, start, end)
            node_idx = node_idx + 1

        # Perform cluster meet
        for node_idx in range(0, self.CLUSTER_SIZE):
            self.cluster_meet(node_idx, self.CLUSTER_SIZE)

        waiters.wait_for_equal(
            lambda: self._wait_for_meet(
                self.CLUSTER_SIZE + (self.CLUSTER_SIZE * replica_count)
            ),
            True,
            timeout=10,
        )

        # Wait for the cluster to be up
        for rg in self.replication_groups:
            logging.info(
                f"Waiting for cluster to change state...{rg.primary.logfile}"
            )
            self.wait_for_logfile(
                rg.primary.logfile, "Cluster state changed: ok"
            )
            rg.setup_replications_cluster()
        logging.info("Cluster is up and running!")
        yield

        # Cleanup
        for rg in self.replication_groups:
            ReplicationGroup.cleanup(rg)

    def get_config_file_lines(self, test_dir, port) -> List[str]:
        module_path = self.resolve_module_path()
        return [
            "enable-debug-command yes",
            f"dir {test_dir}",
            "cluster-enabled yes",
            f"cluster-config-file nodes_{port}.conf",
            f"loadmodule {module_path}",
        ]

    def _wait_for_meet(self, count: int) -> bool:
        for primary in self.replication_groups:
            if not primary._wait_for_meet(count):
                return False
        return True

    def get_primary(self, index) -> ValkeyServerHandle:
        return self.replication_groups[index].primary.server

    def get_primary_port(self, index) -> int:
        return self.replication_groups[index].primary.server.port

    def new_client_for_primary(self, index) -> Valkey:
        return self.replication_groups[index].primary.server.get_new_client()

    def client_for_primary(self, index) -> Valkey:
        return self.replication_groups[index].primary.client

    def get_replication_group(self, index) -> ReplicationGroup:
        return self.replication_groups[index]

    def get_primary_connection(self) -> Valkey:
        return self.client

    def start_new_server(self, is_primary=True) -> Node:
        """Create a new Valkey server instance"""
        module_path = self.resolve_module_path()
        server, client, logfile = self.start_server(
            self.get_bind_port(),
            self.test_name,
            module_path,
            False,
            is_primary,
        )
        return Node(client=client, server=server, logfile=logfile)

    def new_cluster_client(self) -> ValkeyCluster:
        """Return a cluster client"""
        startup_nodes = []
        for index in range(0, self.CLUSTER_SIZE):
            startup_nodes.append(
                ClusterNode(
                    self.replication_groups[index].primary.server.bind_ip,
                    self.replication_groups[index].primary.server.port,
                )
            )

        valkey_conn = ValkeyCluster.from_url(
            url="valkey://{}:{}".format(
                startup_nodes[0].host, startup_nodes[0].port
            ),
            connection_class=Connection,
            startup_nodes=startup_nodes,
            require_full_coverage=True,
        )
        valkey_conn.ping()
        return valkey_conn


def EnableDebugMode(config: List[str]):
    # turn "loadmodule xx.so" into "loadmodule xx.so debug-mode yes"
    #
    # The argument is the module config's bare name: module load arguments are matched
    # verbatim against the registered name, so the "ts." prefix used by CONFIG GET/SET must
    # not appear here.
    module_path = os.path.abspath(os.getenv('MODULE_PATH') or get_module_path() or DEFAULT_MODULE_PATH)
    load_module = f"loadmodule {module_path}"
    return [x.replace(load_module, load_module + " debug-mode yes") for x in config]


class ValkeyTimeSeriesTestCaseDebugMode(ValkeyTimeSeriesTestCaseBase):
    """
    Same as ValkeyTimeSeriesTestCase, except that "debug-mode" is enabled.
    """

    MODULE_ARGS = "debug-mode yes"

    def get_config_file_lines(self, test_dir, port) -> List[str]:
        return EnableDebugMode(super(ValkeyTimeSeriesTestCaseDebugMode, self).get_config_file_lines(test_dir, port))


class ValkeyTimeSeriesClusterTestCaseDebugMode(ValkeyTimeSeriesClusterTestCase):
    """
    Same as ValkeyTimeSeriesClusterTestCase, except that "debug-mode" is enabled.
    """
    def get_config_file_lines(self, test_dir, port) -> List[str]:
        return EnableDebugMode(
            super(ValkeyTimeSeriesClusterTestCaseDebugMode, self).get_config_file_lines(test_dir, port))

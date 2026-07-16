from __future__ import annotations

import os
import re
from sys import platform
from dataclasses import dataclass, field
from typing import Optional, List, Any

from valkey.commands.timeseries.utils import list_to_dict

CWD = os.path.dirname(os.path.realpath(__file__))
ROOT_PATH = os.path.abspath(os.path.join(CWD, ".."))

WORK_DIR = 'work'
RDB_PATH = os.path.join(CWD, 'rdbs')

PORT = 6379
SERVER_VERSION = os.environ.get('SERVER_VERSION', 'unstable')
SERVER_PATH = f"{os.path.dirname(os.path.realpath(__file__))}/build/binaries/{SERVER_VERSION}/valkey-server"

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
SERVER_VERSION = os.environ.get("SERVER_VERSION", "unstable")
VALKEY_SERVER_PATH = f"{SCRIPT_DIR}/build/binaries/{SERVER_VERSION}/valkey-server"
TEST_DIR = f"{ROOT_PATH}/test-data"
LOGS_DIR = f"{TEST_DIR}/logs"

if "VALKEY_SERVER_PATH" in os.environ:
    VALKEY_SERVER_PATH = os.environ["VALKEY_SERVER_PATH"]

if "LOGS_DIR" in os.environ:
    LOGS_DIR = os.environ["LOGS_DIR"]

def get_platform():
    return platform.lower()

def get_dynamic_lib_extension():
    system = get_platform()

    if system == "windows":
        return ".dll"
    elif system == "darwin":
        return ".dylib"
    elif system == "linux":
        return ".so"
    else:
        raise Exception(f"Unsupported platform: {system}")

PLATFORM = get_platform()

# Default MODULE_PATH selection:
# - If the environment variable MODULE_PATH is set, use it.
# - Otherwise prefer the release build artifact (target/release/...), but fall
#   back to the debug build if the release artifact doesn't exist. This makes
#   integration tests pick up the release dylib built by `cargo build --release`
#   while still allowing overrides via the environment.
def _default_module_path():
    env_path = os.environ.get('MODULE_PATH')
    if env_path:
        return os.path.abspath(env_path)

    ext = get_dynamic_lib_extension()
    release_path = os.path.abspath(f"{ROOT_PATH}/target/release/libvalkey_timeseries{ext}")
    debug_path = os.path.abspath(f"{ROOT_PATH}/target/debug/libvalkey_timeseries{ext}")

    if os.path.exists(release_path):
        return release_path
    return debug_path

MODULE_PATH = _default_module_path()

def get_server_version():
    version = os.getenv('VALKEY_VERSION')
    if version is None:
        version = "unstable"

    return version

def get_server_path(version):
    if version is None:
        version = get_server_version()
    path = os.path.join(CWD, "..", ".build/binaries/{}/valkey-server".format(version))
    return os.path.abspath(path)

def get_module_path():
    # Respect explicit environment override first
    path = os.environ.get('MODULE_PATH')
    if path:
        return os.path.abspath(path)

    # Prefer release artifact if available, otherwise use debug.
    ext = get_dynamic_lib_extension()
    release = os.path.abspath(os.path.join(ROOT_PATH, f"target/release/libvalkey_timeseries{ext}"))
    debug = os.path.abspath(os.path.join(ROOT_PATH, f"target/debug/libvalkey_timeseries{ext}"))
    if os.path.exists(release):
        return release
    return debug

def parse_info_response(response):
    """Helper function to parse TS.INFO list response into a dictionary."""

    # Keys that contain integer values
    integer_keys = {
        'totalSamples',
        'memoryUsage',
        'firstTimestamp',
        'lastTimestamp',
        'retentionTime',
        'chunkCount',
        'chunkSize'
    }

    info_dict = {}
    it = iter(response)
    for key in it:
        key_str = key.decode('utf-8')
        value = next(it)
        if key_str in integer_keys:
            # Convert to integer if the key is in the integer keys set
            if isinstance(value, bytes):
                info_dict[key_str] = int(value.decode('utf-8'))
            else:
                info_dict[key_str] = int(value)
        if key_str == 'rules':
            # Handle rules separately
            info_dict[key_str] = []
            for rule in value:
                if isinstance(rule, list):
                    # Convert each rule to a CompactionRule object
                    data = CompactionRule(rule[0], rule[1], rule[2], rule[3])
                    info_dict[key_str].append(data)
            continue
        if isinstance(value, list):
            # Handle nested structures like labels and chunks
            if key_str == 'labels':
                # Convert the labels from a list to a dictionary
                if value is None or len(value) == 0:
                    info_dict['labels'] = {}
                else:
                    info_dict['labels'] = list_to_dict(value)
            elif key_str == 'rules':
                # Convert the rules from a list to a dictionary
                if value is None or len(value) == 0:
                    info_dict[key_str] = []
                else:
                    info_dict[key_str] = []
                    for rule in value:
                        rule_dict = {}
                        for i in range(0, len(rule), 2):
                            rule_dict[rule[i].decode('utf-8')] = rule[i + 1]
                        info_dict[key_str].append(rule_dict)
            elif key_str == 'Chunks':
                if value is None or len(value) == 0:
                    info_dict['chunks'] = []
                else:
                    info_dict['chunks'] = []
                    for chunk in value:
                        chunk_dict = {}
                        for i in range(0, len(chunk), 2):
                            chunk_dict[chunk[i].decode('utf-8')] = chunk[i + 1]
                        info_dict['chunks'].append(chunk_dict)
            else: # Fallback for unknown list types
                info_dict[key_str] = value
        elif isinstance(value, bytes):
            info_dict[key_str] = value.decode('utf-8')
        else:
            info_dict[key_str] = value
    return info_dict


class CompactionRule:
    """Represents a compaction rule for time series."""
    def __init__(self, dest_key, bucket_duration, aggregation, alignment = 0):
        self.dest_key = dest_key.decode('utf-8') if isinstance(dest_key, bytes) else dest_key
        self.bucket_duration = int(bucket_duration)
        self.aggregation = aggregation.decode('utf-8') if isinstance(aggregation, bytes) else aggregation
        if alignment is None:
            self.alignment = 0
        else:
            self.alignment = int(alignment)

    def __key(self):
        # Aggregator identity is case-insensitive: `avg` and `AVG` denote the
        # same aggregator. TS.INFO reports it uppercase (matching
        # RedisTimeSeries); rule equality here should not depend on that case.
        return self.dest_key, self.bucket_duration, self.aggregation.lower(), self.alignment

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, list):
            if len(other) != 4:
                return False
            other = CompactionRule(other[0], other[1], other[2], other[3])
        elif isinstance(other, object):
            if not hasattr(other, 'dest_key') or not hasattr(other, 'bucket_duration') or \
                    not hasattr(other, 'aggregation') or not hasattr(other, 'alignment'):
                return False
            other = CompactionRule(other.dest_key, other.bucket_duration, other.aggregation, other.alignment)
        elif not isinstance(other, CompactionRule):
            return False
        return (self.dest_key == other.dest_key and
                self.bucket_duration == other.bucket_duration and
                self.aggregation.lower() == other.aggregation.lower() and
                self.alignment == other.alignment)
    def __repr__(self):
        return f"CompactionRule(dest_key={self.dest_key}, bucket_duration={self.bucket_duration}, " \
               f"aggregation={self.aggregation}, alignment={self.alignment})"


class ValkeyInfo:
    """Contains information about a point in time of Valkey"""
    def __init__(self, info):
        self.info = info

    def is_save_in_progress(self):
        """Return True if there is a save in progress."""
        return self.info['rdb_bgsave_in_progress'] == 1

    def is_aof_rewrite_in_progress(self):
        """Return True if there is a aof rewrite in progress."""
        return self.info['aof_rewrite_in_progress'] == 1

    def get_master_repl_offset(self):
        return self.info['master_repl_offset']

    def get_master_replid(self):
        return self.info['master_replid']

    def get_replica_repl_offset(self):
        return self.info['slave_repl_offset']

    def is_master_link_up(self):
        """Returns True if role is slave and master_link_status is up"""
        if self.info['role'] == 'slave' and self.info['master_link_status'] == 'up':
            return True
        return False

    def num_replicas(self):
        return self.info['connected_slaves']

    def num_replicas_online(self):
        count=0
        for k,v in self.info.items():
            if re.match('^slave[0-9]', k) and v['state'] == 'online':
                count += 1
        return count

    def was_save_successful(self):
        return self.info['rdb_last_bgsave_status'] == 'ok'

    def was_aofrewrite_successful(self):
        return self.info['aof_last_bgrewrite_status'] == 'ok'

    def used_memory(self):
        return self.info['used_memory']

    def maxmemory(self):
        return self.info['maxmemory']

    def maxmemory_policy(self):
        return self.info['maxmemory_policy']

    def uptime_in_secs(self):
        return self.info['uptime_in_seconds']


def parse_stats_response(response):
    """
    Parse the response from TS.STATS command into a dictionary with proper types.

    Args:
        response: The response from TS.STATS command

    Returns:
        dict: Parsed statistics with proper field types
    """
    if not response:
        return {}

    stats = {}

    int_fields = {
        'totalSeries',
        'totalLabels',
        'totalLabelValuePairs'
    }

    value_fields = {
        'seriesCountByMetricName',
        'labelValueCountByLabelName',
        'seriesCountByLabelValuePair'
    }


    # Convert response to dict if needed
    if isinstance(response, list):
        for i in range(len(response) // 2):
            key = response[2 * i]
            key_str = key.decode('utf-8')
            value = response[2 * i + 1]
            if key_str in int_fields:
                stats[key_str] = int(value)
            elif key_str in value_fields:
                if isinstance(value, list):
                    sub_obj = []
                    for item in value:
                        sub_key = item[0].decode('utf-8')
                        sub_obj.append([sub_key, int(item[1])])
                    stats[key_str] = sub_obj
                else:
                    stats[key_str] = value
            else:   
                stats[key_str] = value
 
    return stats


_EXPECTED_RESP2_MAP_KEYS = frozenset({"results", "has_more", b"results", b"has_more"})


def _try_unwrap_resp2_map(response):
    """Detect and convert a RESP2-encoded map (flat alternating key-value list) to a dict.

    In RESP2, a server-side ``ValkeyValue::Map`` is serialised as a flat list of
    2*N elements – alternating keys and values – instead of a native Python dict.
    We identify this pattern by:
      1. The response is a list with an even, non-zero number of elements.
      2. All even-indexed "key" positions are strings/bytes from the known set
         {``results``, ``has_more``}.
      3. Both ``results`` and ``has_more`` keys are present.

    Requiring both expected keys (and no unexpected ones) prevents mis-detecting
    legitimate legacy result arrays that happen to contain the string ``"results"``
    as a label value at an even index.

    If the pattern matches, a plain Python dict is returned.  Otherwise ``None``
    is returned so callers can fall through to other handling.

    HashMap key order is unspecified, so both orderings are handled:
      ``[b"has_more", 0,  b"results", [...]]``
      ``[b"results",  [...], b"has_more", 0]``
    """
    if not isinstance(response, list) or len(response) < 2 or len(response) % 2 != 0:
        return None

    seen_keys = set()
    for i in range(0, len(response), 2):
        key = response[i]
        # Metadata payloads are often nested lists (e.g. [[value, score, card], ...]).
        # Those are not RESP2 map keys, so bail out early instead of hashing them.
        if not isinstance(key, (str, bytes)):
            return None
        if key not in _EXPECTED_RESP2_MAP_KEYS:
            return None
        seen_keys.add(key)

    # Normalise to str keys for the presence check
    normalised = {k.decode() if isinstance(k, bytes) else k for k in seen_keys}
    if "results" not in normalised or "has_more" not in normalised:
        return None

    result = {}
    for i in range(0, len(response), 2):
        result[response[i]] = response[i + 1]
    return result


def parse_labelsearch_response(response):
    """
    Normalize TS.LABELVALUES server response and return a list of `LabelValue`.

    The server may return either:
      - legacy: a bare array of bulkstrings [b'val1', b'val2', ...], or
      - new: a map { 'has_more': bool, 'results': [...] } where results may contain
        either bulkstrings or arrays with metadata [value, score, cardinality].

    This helper returns a list of `LabelValue` objects. It delegates to
    `LabelValue.parse_response` so both forms are normalized the same way.
    """
    # Delegate to LabelValue.parse_response for uniform behavior
    return LabelValue.parse_response(response)


@dataclass
class LabelValue:
    """Represents a single value returned from TS.LABELVALUES.

    Fields:
      value: the raw value (bytes or str depending on server binding)
      score: optional numeric score (when server returns value metadata)
      cardinality: optional integer cardinality (when server returns value metadata)
    """
    value: Any
    score: Optional[float] = None
    cardinality: Optional[int] = None

    @classmethod
    def parse_response(cls, response) -> List["LabelValue"]:
        """
        Parse a TS.LABELVALUES server response into a list of LabelValue objects.

        The server may return either:
          - legacy: a bare array of bulkstrings [b'val1', b'val2', ...], or
          - new: a map { 'has_more': bool, 'results': [...] } where results may contain
            either bulkstrings or arrays with metadata [value, score, cardinality].

        This method normalizes both forms and always returns a list of LabelValue.
        """
        if not response:
            return []

        # In RESP2 a ValkeyValue::Map arrives as a flat alternating key-value
        # list.  Detect and convert that before the normal dict branch.
        if not isinstance(response, dict):
            unwrapped = _try_unwrap_resp2_map(response)
            if unwrapped is not None:
                response = unwrapped

        # If server returned a map-like object, extract the results key.
        if isinstance(response, dict):
            results = response.get(b"results") or response.get("results") or []
        else:
            results = response

        parsed: List[LabelValue] = []
        for item in results:
            if isinstance(item, (list, tuple)) and len(item) > 0:
                # metadata form: [value, score?, cardinality?]
                value = item[0]
                score = None
                cardinality = None
                if len(item) > 1:
                    try:
                        score = float(item[1])
                    except Exception:
                        score = item[1]
                if len(item) > 2:
                    try:
                        cardinality = int(item[2])
                    except Exception:
                        cardinality = None
                parsed.append(cls(value=value, score=score, cardinality=cardinality))
            else:
                parsed.append(cls(value=item))

        return parsed


@dataclass
class LabelSearchResponse:
    """Encapsulates the full TS.LABELVALUES/TS.LABELNAMES/TS.METRICNAMES response.

    Fields:
      has_more: whether the server indicates more results are available
      results: list of LabelValue entries
    """
    has_more: bool = False
    results: List[LabelValue] = field(default_factory=list)

    def contains_value(self, value: bytes) -> bool:
        """Check if the response contains a specific label value."""
        return any(lv.value == value for lv in self.results)

    @classmethod
    def parse(cls, response) -> "LabelSearchResponse":
        """Parse a server response (legacy list or dict with metadata) into a
        LabelValuesResponse instance."""
        if not response:
            return cls(has_more=False, results=[])

        # In RESP2 a ValkeyValue::Map arrives as a flat alternating key-value
        # list.  Detect and convert that before the normal dict branch so that
        # both has_more and results are extracted correctly.
        if not isinstance(response, dict):
            unwrapped = _try_unwrap_resp2_map(response)
            if unwrapped is not None:
                response = unwrapped

        # If server returned a map-like response with metadata
        if isinstance(response, dict):
            # Extract has_more (can be bool, int, or bytes)
            raw_has_more = response.get(b"has_more") if (b"has_more" in response) else response.get("has_more")
            has_more = False
            if raw_has_more is not None:
                if isinstance(raw_has_more, bytes):
                    try:
                        has_more = raw_has_more.decode('utf-8').lower() in ("1", "true")
                    except Exception:
                        has_more = bool(raw_has_more)
                else:
                    has_more = bool(raw_has_more)

            raw_results = response.get(b"results") or response.get("results") or []
            results = LabelValue.parse_response(raw_results)
            return cls(has_more=has_more, results=results)

        # Legacy response: assume plain list of bulkstrings -> no has_more
        results = LabelValue.parse_response(response)
        return cls(has_more=False, results=results)

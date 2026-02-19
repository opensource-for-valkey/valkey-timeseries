"""Quick smoke-test for ts_query_result data classes (no server needed)."""
import sys, os

sys.path.insert(0, os.path.dirname(__file__))

from query_result import (
    QueryResult, QuerySample,
    VectorSample, MatrixSample,
    ScalarResult, StringResult,
)

# ── RESP3-style (dict keys / values) ──────────────────────────────────────────

# vector
raw_vec = {
    b"resultType": b"vector",
    b"result": [
        {b"metric": {b"__name__": b"http_requests", b"service": b"api"}, b"value": [1000, 100.0]},
        {b"metric": {b"__name__": b"http_requests", b"service": b"db"}, b"value": [2000, 50.0]},
    ],
}
qr = QueryResult.from_raw(raw_vec)
assert qr.is_vector(), f"expected vector, got {qr.result_type!r}"
assert len(qr.result) == 2
s0 = qr.result[0]
assert isinstance(s0, VectorSample)
assert s0.name == "http_requests"
assert s0.label("service") == "api"
assert s0.value.timestamp_ms == 1000
assert s0.value.value == 100.0
print("PASS vector (RESP3)")

# matrix
raw_mat = {
    b"resultType": b"matrix",
    b"result": [
        {b"metric": {b"__name__": b"up"}, b"value": [[1000, 1.0], [2000, 1.0], [3000, 0.0]]}
    ],
}
qr2 = QueryResult.from_raw(raw_mat)
assert qr2.is_matrix()
m0 = qr2.result[0]
assert isinstance(m0, MatrixSample)
assert m0.name == "up"
assert len(m0.values) == 3
assert m0.values[2].value == 0.0
print("PASS matrix (RESP3)")

# scalar
raw_scalar = {b"resultType": b"scalar", b"result": [5000, 42.0]}
qr3 = QueryResult.from_raw(raw_scalar)
assert qr3.is_scalar()
assert isinstance(qr3.result, ScalarResult)
assert qr3.result.timestamp_ms == 5000
assert qr3.result.value == 42.0
print("PASS scalar (RESP3)")

# string
raw_str = {b"resultType": b"string", b"result": [5000, b"hello"]}
qr4 = QueryResult.from_raw(raw_str)
assert qr4.is_string()
assert isinstance(qr4.result, StringResult)
assert qr4.result.timestamp_ms == 5000
assert qr4.result.value == "hello"
print("PASS string (RESP3)")

# ── RESP2-style (flat interleaved lists) ──────────────────────────────────────

# vector – outer map is a flat list; metric map is also a flat list
raw_vec_resp2 = [
    b"resultType", b"vector",
    b"result", [
        [b"metric", [b"__name__", b"http_requests", b"env", b"prod"], b"value", [3000, 75.5]],
    ],
]
qr5 = QueryResult.from_raw(raw_vec_resp2)
assert qr5.is_vector()
s5 = qr5.result[0]
assert isinstance(s5, VectorSample)
assert s5.name == "http_requests"
assert s5.label("env") == "prod"
assert s5.value.timestamp_ms == 3000
assert s5.value.value == 75.5
print("PASS vector (RESP2)")

# matrix – flat list maps
raw_mat_resp2 = [
    b"resultType", b"matrix",
    b"result", [
        [b"metric", [b"__name__", b"rate"], b"value", [[100, 1.5], [200, 2.5]]],
    ],
]
qr6 = QueryResult.from_raw(raw_mat_resp2)
assert qr6.is_matrix()
m6 = qr6.result[0]
assert m6.name == "rate"
assert len(m6.values) == 2
assert m6.values[1].timestamp_ms == 200
assert m6.values[1].value == 2.5
print("PASS matrix (RESP2)")

# empty vector
raw_empty = {b"resultType": b"vector", b"result": []}
qr7 = QueryResult.from_raw(raw_empty)
assert qr7.is_vector()
assert qr7.result == []
print("PASS empty vector")

# unknown resultType raises ValueError
try:
    QueryResult.from_raw({b"resultType": b"unknown", b"result": []})
    assert False, "should have raised ValueError"
except ValueError:
    print("PASS ValueError on unknown resultType")

print("\nAll assertions passed.")

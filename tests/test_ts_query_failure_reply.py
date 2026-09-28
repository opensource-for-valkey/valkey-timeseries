"""A PromQL evaluation that fails unexpectedly must still answer its client.

`TS.QUERY` / `TS.QUERYRANGE` block the client and evaluate on a query worker. A panic in the
evaluation unwinds through the worker's job, whose blocked-client handle then unblocked the client
without having written a reply. The server delivers nothing, so the client waits forever, or, if
it pipelines, reads the next command's reply as this one's.

`TS._DEBUG PANIC_NEXT_EVALUATION` (debug mode only) makes the next evaluation panic on the
evaluation pool, the path a real bug would take.
"""
import pytest
from valkey import ResponseError, StrictValkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util.waiters import wait_for_true
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseDebugMode


class TestQueryFailureStillReplies(ValkeyTimeSeriesTestCaseDebugMode):

    def _client(self):
        # A missing reply shows up as a socket timeout instead of hanging the suite.
        return StrictValkey(host=self.server.bind_ip, port=self.server.port, socket_timeout=10)

    def _series(self, client):
        client.execute_command("TS.CREATE", "m", "LABELS", "__name__", "m", "job", "a")
        for t in (1000, 2000, 3000):
            client.execute_command("TS.ADD", "m", t, t)

    @pytest.mark.parametrize("command", [
        ("TS.QUERY", "m", "TIME", "3000"),
        ("TS.QUERYRANGE", "m", "START", "1000", "END", "3000", "STEP", "1s"),
    ])
    def test_panicking_evaluation_answers_with_an_error(self, command):
        client = self._client()
        self._series(client)

        assert client.execute_command("TS._DEBUG", "PANIC_NEXT_EVALUATION") in (b"OK", "OK", True)
        with pytest.raises(ResponseError, match="internal error"):
            client.execute_command(*command)

        # The cause is in the server log, as the error says.
        wait_for_true(lambda: self.server.verify_string_in_logfile(
            "job panicked: TS._DEBUG PANIC_NEXT_EVALUATION"))

        # The connection is still in step, and the next query is unaffected.
        assert client.ping()
        assert client.execute_command(*command)
        assert self.server.is_alive()

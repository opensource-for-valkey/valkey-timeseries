from valkeytestframework.util.waiters import *
from valkey_timeseries_test_case import ValkeyTimeSeriesTestCaseBase
from valkeytestframework.conftest import resource_port_tracker


class TestTimeSeriesBasic(ValkeyTimeSeriesTestCaseBase):

    def test_basic(self):
        client = self.server.get_new_client()
        # Validate that the valkey-timeseries module is loaded.
        module_list_data = client.execute_command('MODULE LIST')

        module_loaded = False
        for module in module_list_data:
            if module[b'name'] == b'ts':
                module_loaded = True
                break
        assert (module_loaded)
        # Validate that all the TS.* commands are supported on the server.
        command_cmd_result = client.execute_command('COMMAND')
        ts_cmds = ["TS.CREATE", "TS.ALTER", "TS.ADD", "TS.MADD", "TS.DEL", "TS.GET", "TS.MGET", "TS.RANGE", "TS.MRANGE",
                   "TS.CARD", "TS.QUERYINDEX", "TS.LABELSTATS", "TS.LABELNAMES", "TS.LABELVALUES", "TS.INFO"]
        assert all(item.lower() in command_cmd_result for item in ts_cmds)
        # Basic timeseries create, item add and item exists validation.
        ts_add_result = client.execute_command('TS.ADD series1 1000 102')
        assert ts_add_result == 1000
        exists_result = client.execute_command('TS.GET series1')
        assert exists_result == [1000, b'102']

    def test_timeseries_create_cmd(self):
        client = self.server.get_new_client()
        # cmd create
        assert client.execute_command(
            'TS.CREATE temperature:2:32 RETENTION 60000 DUPLICATE_POLICY MAX LABELS sensor_id 2 area_id 32') == b'OK'
        assert client.execute_command('TS.ADD temperature:2:32 1000 72.5') == 1000

    def test_module_data_type(self):
        # Validate the name of the Module data type.
        client = self.server.get_new_client()
        assert client.execute_command('TS.ADD series 2000 45.0') == 2000
        type_result = client.execute_command('TYPE series')
        assert type_result == b"TSDB-TYPE"
        # Validate the name of the Module data type.
        encoding_result = client.execute_command('OBJECT ENCODING series')
        assert encoding_result == b"raw"

    def test_timeseries_obj_access(self):
        client = self.server.get_new_client()
        # check timeseries with basic valkey command
        # cmd touch
        assert client.execute_command('TS.ADD key1 1000 2.0') == 1000
        assert client.execute_command('TS.ADD key2 1000 3.0') == 1000
        assert client.execute_command('TOUCH key1 key2') == 2
        assert client.execute_command('TOUCH key3') == 0
        self.verify_server_key_count(client, 2)
        assert client.execute_command('DBSIZE') == 2
        random_key = client.execute_command('RANDOMKEY')
        assert random_key == b"key1" or random_key == b"key2"

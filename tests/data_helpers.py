import calendar
import csv
import json
import math
import os.path
import datetime
import os
import tempfile
import zipfile
from datetime import datetime
from typing import List, Optional

PIPELINE_SIZE = 1000
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')

PC_Timestamp = 'timestamp'
PC_Region = 'region'
PC_LocationType = 'location_type'
PC_Consumption = 'consumption'

class TemperatureRecord:
    def __init__(self, sensor_id, air_temp, day, hour, install_type, borough, nta_code):
        self.sensor_id = sensor_id
        self.air_temp = air_temp
        self.day = day
        self.hour = hour
        self.timestamp = self._create_timestamp(day, hour)
        self.install_type = install_type
        self.borough = borough
        self.nta_code = nta_code

    def __repr__(self):
        return (f"TemperatureRecord(sensor_id={self.sensor_id}, air_temp={self.air_temp}, "
                f"day={self.day}, hour={self.hour}, install_type={self.install_type}, "
                f"borough={self.borough}, nta_code={self.nta_code})")
    def key(self):
        return 'ny_temps:{}'.format(self.nta_code)

    def _create_timestamp(self, day, hour):
        date_time_obj = datetime.strptime(day, '%m/%d/%Y')
        date_time_obj = date_time_obj.replace(hour=int(hour))
        return calendar.timegm(date_time_obj.timetuple()) * 1000

    def metric(self):
        return ('ny_temps{{nta_code="{}",sensor_id="{}",borough="{}",install_type="{}"}}'
                .format(self.nta_code, self.sensor_id, self.borough, self.install_type))

# ['Sensor.ID', 'AirTemp', 'Day', 'Hour', 'Install.Type', 'Borough', 'ntacode']
# ['Bk-BR_01', '71.189', '06/15/2018', '1', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '70.24333333', '06/15/2018', '2', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '69.39266667', '06/15/2018', '3', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '68.26316667', '06/15/2018', '4', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '67.114', '06/15/2018', '5', 'Street Tree', 'Brooklyn', 'BK81']
# ['Bk-BR_01', '65.9655', '06/15/2018', '6', 'Street Tree', 'Brooklyn', 'BK81']


def load_temperature_data():
    # column indexes
    sensor_id_column_idx = 0
    air_temp_column_idx = 1
    day_column_idx = 2
    hour_column_idx = 3
    install_type_column_idx = 4
    borough_column_idx = 5
    nta_code_column_idx = 6

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        data_path = os.path.join(DATA_DIR, 'Hyperlocal_Temperature_Monitoring_20241012_1M.zip')
        # Open the ZIP file
        with zipfile.ZipFile(data_path, 'r') as zip_ref:
            # Extract all contents to the temporary directory
            zip_ref.extractall(temp_dir)

        # Find the CSV file in the temporary directory
        csv_file = next(file for file in os.listdir(temp_dir) if file.endswith('.csv'))
        csv_path = os.path.join(temp_dir, csv_file)

        # Parse the timestamp and convert it to Unix timestamp
        # Read and print the CSV contents
        with open(csv_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            for row in csv_reader:
                sensor_id = row[sensor_id_column_idx]
                if sensor_id == 'Sensor.ID':
                    continue

                air_temp = row[air_temp_column_idx]
                day = row[day_column_idx]
                hour = row[hour_column_idx]
                install_type = row[install_type_column_idx]
                install_type = install_type.lower().replace(' ', '_')
                borough = row[borough_column_idx]
                nta_code = row[nta_code_column_idx]

                # print(f"Sensor ID: {sensor_id}, Air Temp: {air_temp}, Day: {day}, Hour: {hour}, ")
                # Create a TemperatureRecord object
                record = TemperatureRecord(sensor_id, air_temp, day, hour, install_type, borough, nta_code)

                yield record

class PowerConsumptionRecord:
    def __init__(self, timestamp, region, location_type, consumption):
        self.timestamp = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        self.region = region
        self.location_type = location_type
        self.consumption = float(consumption)

    def __repr__(self):
        return (f"PowerConsumptionRecord(timestamp={self.timestamp}, region={self.region}, "
                f"location_type={self.location_type}, consumption={self.consumption})")
    def key(self):
        return 'power_consumption:{}::{}'.format(self.region, self.location_type)
    def metric(self):
        return ('power_consumption{{region="{}",location_type="{}"}}'
                .format(self.region, self.location_type))


def load_json_rows(file_path):
    """
    Generator function to load rows from a JSON file.

    Args:
    file_path (str): Path to the JSON file.

    Yields:
    dict: Each row from the JSON file as a dictionary.
    """
    try:
        with open(file_path, 'r') as file:
            # Load the entire JSON content
            data = json.load(file)

            # Check if the loaded data is a list
            if isinstance(data, list):
                for row in data:
                    yield row
            # If it's a dictionary, yield it as a single item
            elif isinstance(data, dict):
                yield data
            else:
                raise ValueError("JSON file must contain a list of objects or a single object")

    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except IOError as e:
        print(f"I/O error({e.errno}): {e.strerror}")
    except Exception as e:
        print(f"Unexpected error: {e}")



def load_power_consumption_data():
    data_path = os.path.join(DATA_DIR, 'power_consumption.json')
    with open(data_path, 'r') as f:
        data = json.load(f)
        res = {}
        for row in data:
            ts = row[PC_Timestamp]
            region = row[PC_Region]
            location_type = row[PC_LocationType]
            consumption = row[PC_Consumption]

            group_key = region + ':' + location_type
            if group_key not in res:
                res[group_key] = []

            res[group_key].append([ts, consumption])

        return res


def load_text_rows(file_path):
    """
    Generator function to load rows from a text file.

    Args:
    file_path (str): Path to the text file.

    Yields:
    str: Each row from the text file.
    """
    try:
        with open(file_path, 'r') as file:
            for row in file:
                yield row
    except IOError as e:
        print(f"I/O error({e.errno}): {e.strerror}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def load_samples_from_file(filename, start=None, step=None):
    """
    Load a series from a file.

    Args:
    filename (str): Path to the file.

    Returns:
    list: A list of series data.
    """
    if start is None:
        start = datetime.now() - datetime.timedelta(days=1)  # Load data for the previous day

    # Calculate the interval between each data point
    if step is None:
        step = datetime.timedelta(minutes=5)

    timestamp = start
    for row in load_text_rows(filename):
        yield [timestamp, row]
        timestamp += step

def load_cpu_data(start=None, step=None):
    data_path = os.path.join(DATA_DIR, 'cpu-values.txt')
    return load_samples_from_file(data_path, start, step)

def load_memory_data(start=None, step=None):
    data_path = os.path.join(DATA_DIR, 'memory-values.txt')
    return load_samples_from_file(data_path, start, step)

def ingest_temperature_data(valkey_conn):
    print("Loading data into valkey...")
    r = valkey_conn.pipeline(transaction=False)
    count = 0
    added_keys = set([])

    print("Loading rows...")

    for row in load_temperature_data():
        # print("Loading row: ", row)
        if row.timestamp < 0:
            continue

        temperature = row.air_temp
        if temperature is None:
            continue

        if count > PIPELINE_SIZE:
            r.execute()
            count = 0
            r = valkey_conn.pipeline(transaction=False)

        # Create series if not already exists
        key = row.key()
        if key not in added_keys:
            added_keys.add(key)
            metric = row.metric()
            valkey_conn.execute_command('TS.CREATE', key,
                                        'ENCODING', 'COMPRESSED',
                                        'CHUNK_SIZE_BYTES', '8ki',
                                        'DECIMAL_DIGITS', 2,
                                        'LABELS', 'sensor_id', row.sensor_id, 'borough', row.borough,
                                        'install_type', row.install_type, 'nta_code', row.nta_code)
            print(f"Created series: {key}, metric={metric}")

        r.execute_command('TS.ADD', key, row.timestamp, temperature)
        count += 1

    r.execute()


def ingest_power_consumption_data(valkey_conn, encoding='COMPRESSED', chunk_size_bytes='8ki'):
    print("Loading data into valkey...")
    r = valkey_conn.pipeline(transaction=False)
    count = 0

    print("Loading rows...")

    for key, values in load_power_consumption_data().items():
        region, location_type = key.split(':')
        metric = 'power_consumption{{region="{}",location_type="{}"}}'.format(region, location_type)
        valkey_conn.execute_command('TS.CREATE', key, metric, 'ENCODING', encoding, 'CHUNK_SIZE_BYTES', chunk_size_bytes, 'DECIMAL_DIGITS', 1, 'LABELS', 'region', region, 'location_type', location_type)
        print(f"Created series: {key}, metric={metric}")

        for ts, consumption in values:
            if count > PIPELINE_SIZE:
                r.execute()
                count = 0
                r = valkey_conn.pipeline(transaction=False)

            r.execute_command('TS.ADD', key, ts, consumption)
            count += 1

    r.execute()


def _add(client, key: str, start_ms: int, values: List[float],
         step_ms: int = 1000) -> None:
    """Bulk-add a list of values to *key* starting at *start_ms*."""
    for i, v in enumerate(values):
        client.execute_command("TS.ADD", key, start_ms + i * step_ms, v)


def _create_linear_series(client, key: str, start_ms: int = 1000,
                          count: int = 100, slope: float = 2.0,
                          intercept: float = 1.0,
                          step_ms: int = 1000) -> None:
    """Create a time series with a perfect linear trend."""
    values = [intercept + slope * i for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_quadratic_series(client, key: str, start_ms: int = 1000,
                              count: int = 100,
                              step_ms: int = 1000) -> None:
    """Create a time series with a quadratic trend: 0.01*t^2 + 0.5*t + 10."""
    values = [0.01 * (i * i) + 0.5 * i + 10.0 for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_exponential_series(client, key: str, start_ms: int = 1000,
                                count: int = 100,
                                step_ms: int = 1000) -> None:
    """Create a time series with an exponential trend: 2 * exp(0.05 * i)."""
    values = [2.0 * math.exp(0.05 * i) for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_negative_linear_series(client, key: str, start_ms: int = 1000,
                                    count: int = 100,
                                    step_ms: int = 1000) -> None:
    """Create a time series with all-negative values."""
    values = [-(i + 1.0) for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_constant_series(client, key: str, start_ms: int = 1000,
                             count: int = 10, value: float = 5.0,
                             step_ms: int = 1000) -> None:
    """Create a time series with constant values."""
    for i in range(count):
        client.execute_command("TS.ADD", key, start_ms + i * step_ms, value)


def _create_sine_series(client, key: str, start_ms: int = 1000,
                        count: int = 100, step_ms: int = 1000,
                        amplitude: Optional[float] = None,
                        base: Optional[float] = None,
                        period: Optional[int] = None) -> None:
    """Create a time series with sinusoidal values.

    Two modes:
    - Simple (default): values = sin(i * 0.1) for i in range(count)
    - Seasonal (when *period* is provided):
      values = base + amplitude * sin(2 * pi * i / period)
    """
    if period is not None:
        amp = amplitude if amplitude is not None else 20.0
        b = base if base is not None else 100.0
        values = [b + amp * math.sin(2 * math.pi * i / period) for i in range(count)]
    else:
        values = [math.sin(i * 0.1) for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_alternating_series(client, key: str, start_ms: int = 1000,
                                count: int = 20, step_ms: int = 1000) -> None:
    """Create a time series with alternating values."""
    values = [1.0 if i % 2 == 0 else -1.0 for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_daily_seasonal_series(client, key: str, days: int = 30,
                                  start_ms: int = 1000) -> None:
    """Create a series with daily seasonality and an upward trend."""
    values = []
    for day in range(days):
        for hour in range(24):
            # Business-hours pattern
            if 9 <= hour <= 17:
                value = 80.0 + 20.0 * math.sin((hour - 13) * math.pi / 8)
            else:
                value = 40.0 + 10.0 * math.sin(hour * math.pi / 12)
            # Add a gentle upward trend
            value += day * 0.5
            values.append(value)
    _add(client, key, start_ms, values, 3600000)

def _create_quadratic_series(client, key: str, start_ms: int = 1000,
                              count: int = 100,
                              step_ms: int = 1000) -> None:
    """Create a time series with a quadratic trend: 0.01*t^2 + 0.5*t + 10."""
    values = [0.01 * (i * i) + 0.5 * i + 10.0 for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_exponential_series(client, key: str, start_ms: int = 1000,
                                count: int = 100,
                                step_ms: int = 1000) -> None:
    """Create a time series with an exponential trend: 2 * exp(0.05 * i)."""
    values = [2.0 * math.exp(0.05 * i) for i in range(count)]
    _add(client, key, start_ms, values, step_ms)


def _create_negative_linear_series(client, key: str, start_ms: int = 1000,
                                    count: int = 100,
                                    step_ms: int = 1000) -> None:
    """Create a time series with all-negative values."""
    values = [-(i + 1.0) for i in range(count)]
    _add(client, key, start_ms, values, step_ms)
# -*- coding: utf-8 -*-

import requests
import json
from powietrze import base


class APIError(Exception):
    def __init__(self, message):
        self.message = message


def sync_fetch_json(url, timeout):
    try:
        r = requests.get(url, timeout=timeout)

        if r.status_code != 200:
            raise APIError('{} returned {}'.format(url, r.status_code))
        return r.json()
    except requests.exceptions.ConnectionError as err:
        raise APIError('{} connection error: {}'.format(url, err))
    except requests.exceptions.ReadTimeout as err:
        raise APIError('{} timeout error: {}'.format(url, err))
    except json.decoder.JSONDecodeError as err:
        raise APIError("{} returned invalid JSON: {}".format(url, err))


class API(base.BaseAPI):
    def __init__(self, timeout=5):
        super().__init__(timeout)

    def fetch_stations_data(self):
        self._stations_data = sync_fetch_json(self._api_all_stations_url, self._timeout)

    def _create_station_object(self, station, distance=None):
        return super()._create_station_object(StationSync, station, distance)


class StationSync(base.BaseStation):
    def __init__(self, station_id, name, city, lon, lat, timeout=5, distance=None):
        super().__init__(station_id, name, city, lon, lat, timeout, distance)

    def fetch_sensors_data(self):
        self._sensors_data = sync_fetch_json(
            self._api_station_url.format(stationId=self._station_id), self._timeout)

    def fetch_measurements(self):
        measurements = {}
        for sensor_id in self._get_sensors_ids():
            data = sync_fetch_json(self._api_sensor_url.format(sensorId=sensor_id), self._timeout)
            measurements[data['key']] = data['values']

        self._measurements = measurements

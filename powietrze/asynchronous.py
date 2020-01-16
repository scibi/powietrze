# -*- coding: utf-8 -*-

import logging
import aiohttp
import asyncio

from powietrze import base

_LOGGER = logging.getLogger(__name__)


class APIError(Exception):
    def __init__(self, message):
        self.message = message


class API(base.BaseAPI):
    def __init__(self, timeout=5, session=None):
        super().__init__(timeout)
        if session is None:
            async def _create_session():
                return aiohttp.ClientSession()
            loop = asyncio.get_event_loop()
            self._session = loop.run_until_complete(_create_session())
        else:
            self._session = session

    async def fetch_stations_data(self):
        self._stations_data = await self._fetch_json(self._api_all_stations_url, self._timeout)

    def _create_station_object(self, station, distance=None):
        return super()._create_station_object(StationAsync, station, distance)

    async def _fetch_json(self, url, timeout):
        client_timeout = aiohttp.ClientTimeout(total=timeout)
        try:
            async with self._session.get(url, timeout=client_timeout) as resp:
                if resp.status != 200:
                    raise APIError('{} returned {}'.format(url, resp.status))
                return await resp.json()
        except asyncio.TimeoutError:
            raise APIError('{} timeouted'.format(url))
        except aiohttp.client_exceptions.ClientConnectorError as err:
            raise APIError('{} connection error: {}'.format(url, err))
        except aiohttp.client_exceptions.ContentTypeError as err:
            raise APIError("{} didn't return valid JSON: {}".format(url, err))


class StationAsync(base.BaseStation):
    def __init__(self, api, station_id, name, city, lon, lat, timeout=5, distance=None):
        super().__init__(api, station_id, name, city, lon, lat, timeout, distance)

    async def async_fetch_sensors_data(self):
        url = self._api_station_url.format(stationId=self._station_id)
        self._sensors_data = await self._api._fetch_json(url, self._timeout)

    async def async_fetch_measurements(self):
        measurements = {}
        for sensor_id in self._get_sensors_ids():
            url = self._api_sensor_url.format(sensorId=sensor_id)
            data = await self._api._fetch_json(url, self._timeout)
            measurements[data['key']] = data['values']

        self._measurements = measurements

#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `powietrze` package."""

import aiohttp
import asyncio
import pytest
import socket

from collections import namedtuple

from powietrze import asynchronous


class CaseControlledTestServer(aiohttp.test_utils.RawTestServer):
    def __init__(self, **kwargs):
        super().__init__(self._handle_request, **kwargs)
        self._requests = asyncio.Queue()
        self._responses = {}                # {id(request): Future}

    async def close(self):
        ''' cancel all pending requests before closing '''
        for future in self._responses.values():
            future.cancel()
        await super().close()

    async def _handle_request(self, request):
        ''' push request to test case and wait until it provides a response '''
        self._responses[id(request)] = response = asyncio.Future()
        self._requests.put_nowait(request)
        try:
            # wait until test case provides a response
            return await response
        finally:
            del self._responses[id(request)]

    async def receive_request(self):
        ''' wait until test server receives a request '''
        return await self._requests.get()

    def send_response(self, request, *args, **kwargs):
        ''' send web response from test case to client code '''
        response = aiohttp.web.Response(*args, **kwargs)
        self._responses[id(request)].set_result(response)


class FakeResolver:
    def __init__(self):
        self._servers = {}

    def add(self, host, port, target_port):
        self._servers[host, port] = target_port

    async def resolve(self, host, port=0, family=socket.AF_INET):
        try:
            fake_port = self._servers[host, port]
        except KeyError:
            raise OSError('No test server known for %s' % host)
        return [{
            'hostname': host,
            'host': '127.0.0.1',
            'port': fake_port,
            'family': socket.AF_INET,
            'proto': 0,
            'flags': socket.AI_NUMERICHOST,
        }]


_RedirectContext = namedtuple('RedirectContext', 'add_server session')


@pytest.fixture
async def aiohttp_redirector():
    resolver = FakeResolver()
    connector = aiohttp.TCPConnector(resolver=resolver, use_dns_cache=False)
    async with aiohttp.ClientSession(connector=connector) as session:
        yield _RedirectContext(add_server=resolver.add, session=session)

SAMPLE_RESULT_FIND_ALL = """
[
    {
        "id": 114,
        "stationName": "Wroc\u0142aw - Bartnicza",
        "gegrLat": "51.115933",
        "gegrLon": "17.141125",
        "city": {
            "id": 1064,
            "name": "Wroc\u0142aw",
            "commune": {
                "communeName": "Wroc\u0142aw",
                "districtName": "Wroc\u0142aw",
                "provinceName": "DOLNO\u015aL\u0104SKIE"
            }
        },
        "addressStreet": "ul. Bartnicza"
    },
    {
        "id": 117,
        "stationName": "Wroc\u0142aw - Korzeniowskiego",
        "gegrLat": "51.129378",
        "gegrLon": "17.029250",
        "city": {
            "id": 1064,
            "name": "Wroc\u0142aw",
            "commune": {
                "communeName": "Wroc\u0142aw",
                "districtName": "Wroc\u0142aw",
                "provinceName": "DOLNO\u015aL\u0104SKIE"
            }
        },
        "addressStreet": "ul. Wyb. J.Conrada-Korzeniowskiego 18"
    },
    {
        "id": 158,
        "stationName": "Bydgoszcz Warszawska",
        "gegrLat": "53.134083",
        "gegrLon": "17.995708",
        "city": {
            "id": 90,
            "name": "Bydgoszcz",
            "commune": {
                "communeName": "Bydgoszcz",
                "districtName": "Bydgoszcz",
                "provinceName": "KUJAWSKO-POMORSKIE"
            }
        },
        "addressStreet": "ul. Warszawska 10"
    },
    {
        "id": 530,
        "stationName": "Warszawa-Komunikacyjna",
        "gegrLat": "52.219298",
        "gegrLon": "21.004724",
        "city": {
            "id": 1006,
            "name": "Warszawa",
            "commune": {
                "communeName": "Warszawa",
                "districtName": "Warszawa",
                "provinceName": "MAZOWIECKIE"
            }
        },
        "addressStreet": "al. Niepodleg\u0142o\u015bci 227/233"
    }
]"""

SAMPLE_STATION_ID = 530

SAMPLE_RESULT_STATION_SENSORS = """
[
    {
        "id": 3575,
        "stationId": 530,
        "param": {
            "paramName": "benzen",
            "paramFormula": "C6H6",
            "paramCode": "C6H6",
            "idParam": 10
        }
    },
    {
        "id": 3576,
        "stationId": 530,
        "param": {
            "paramName": "tlenek w\u0119gla",
            "paramFormula": "CO",
            "paramCode": "CO",
            "idParam": 8
        }
    },
    {
        "id": 3580,
        "stationId": 530,
        "param": {
            "paramName": "dwutlenek azotu",
            "paramFormula": "NO2",
            "paramCode": "NO2",
            "idParam": 6
        }
    },
    {
        "id": 3584,
        "stationId": 530,
        "param": {
            "paramName": "py\u0142 zawieszony PM10",
            "paramFormula": "PM10",
            "paramCode": "PM10",
            "idParam": 3
        }
    },
    {
        "id": 3585,
        "stationId": 530,
        "param": {
            "paramName": "py\u0142 zawieszony PM2.5",
            "paramFormula": "PM2.5",
            "paramCode": "PM2.5",
            "idParam": 69
        }
    },
    {
        "id": 3762,
        "stationId": 552,
        "param": {
            "paramName": "ozon",
            "paramFormula": "O3",
            "paramCode": "O3",
            "idParam": 5
        }
    },
    {
        "id": 3769,
        "stationId": 552,
        "param": {
            "paramName": "dwutlenek siarki",
            "paramFormula": "SO2",
            "paramCode": "SO2",
            "idParam": 1
        }
    }
]
"""

SAMPLE_RESULT_STATION_SENSORS_SMALL = """
[
    {
        "id": 3575,
        "stationId": 530,
        "param": {
            "paramName": "benzen",
            "paramFormula": "C6H6",
            "paramCode": "C6H6",
            "idParam": 10
        }
    },
    {
        "id": 3584,
        "stationId": 530,
        "param": {
            "paramName": "py\u0142 zawieszony PM10",
            "paramFormula": "PM10",
            "paramCode": "PM10",
            "idParam": 3
        }
    },
    {
        "id": 3585,
        "stationId": 530,
        "param": {
            "paramName": "py\u0142 zawieszony PM2.5",
            "paramFormula": "PM2.5",
            "paramCode": "PM2.5",
            "idParam": 69
        }
    }
]
"""

SAMPLE_RESULT_STATION_DATA_3575 = """
{
    "key": "C6H6",
    "values": [
        {
            "date": "2019-06-25 23:00:00",
            "value": 0.5
        },
        {
            "date": "2019-06-25 22:00:00",
            "value": 1.14
        },
        {
            "date": "2019-06-25 21:00:00",
            "value": 0.48
        },
        {
            "date": "2019-06-25 20:00:00",
            "value": 0.37
        },
        {
            "date": "2019-06-25 19:00:00",
            "value": 0.29
        }
    ]
}
"""
@pytest.mark.asyncio
async def test_api_create_stations_by_names(aiohttp_redirector, loop):
    async with CaseControlledTestServer() as server:
        aiohttp_redirector.add_server('api.gios.gov.pl', 80, server.port)
        api = asynchronous.API(session=aiohttp_redirector.session)

        task = loop.create_task(api.fetch_stations_data())
        request = await server.receive_request()
        assert request.path_qs == '/pjp-api/rest/station/findAll'

        server.send_response(request, text=SAMPLE_RESULT_FIND_ALL, content_type='application/json',)
        await task

        stations = api.create_stations_by_names(["Bydgoszcz Warszawska"])

        assert len(stations) == 1
        assert stations[0].name == 'Bydgoszcz Warszawska'


@pytest.mark.asyncio
async def test_api_create_stations_by_ids(aiohttp_redirector, loop):
    async with CaseControlledTestServer() as server:
        aiohttp_redirector.add_server('api.gios.gov.pl', 80, server.port)
        api = asynchronous.API(session=aiohttp_redirector.session)

        task = loop.create_task(api.fetch_stations_data())
        request = await server.receive_request()
        assert request.path_qs == '/pjp-api/rest/station/findAll'

        server.send_response(request, text=SAMPLE_RESULT_FIND_ALL, content_type='application/json',)
        await task

        stations = api.create_stations_by_ids([117])

        assert len(stations) == 1
        assert stations[0].name == 'Wrocław - Korzeniowskiego'


@pytest.mark.asyncio
async def test_api_create_stations_by_location(aiohttp_redirector, loop):
    async with CaseControlledTestServer() as server:
        aiohttp_redirector.add_server('api.gios.gov.pl', 80, server.port)
        api = asynchronous.API(session=aiohttp_redirector.session)

        task = loop.create_task(api.fetch_stations_data())
        request = await server.receive_request()
        assert request.path_qs == '/pjp-api/rest/station/findAll'

        server.send_response(request, text=SAMPLE_RESULT_FIND_ALL, content_type='application/json',)
        await task

        stations = api.create_stations_by_location(51.129378, 17.029250)

        assert len(stations) == 2
        assert stations[0].name == 'Wrocław - Korzeniowskiego'
        assert stations[1].name == 'Wrocław - Bartnicza'


@pytest.mark.asyncio
async def test_station_get_available_params(aiohttp_redirector, loop):
    async with CaseControlledTestServer() as server:
        aiohttp_redirector.add_server('api.gios.gov.pl', 80, server.port)
        api = asynchronous.API(session=aiohttp_redirector.session)

        station = asynchronous.StationAsync(
            api=api,
            station_id=SAMPLE_STATION_ID,
            name='Warszawa-Komunikacyjna',
            city='Warszawa',
            lon='21.004724',
            lat='52.219298',
            timeout=5,
            distance=3)

        assert station.name == 'Warszawa-Komunikacyjna'

        task = loop.create_task(station.async_fetch_sensors_data())
        request = await server.receive_request()

        assert request.path_qs == '/pjp-api/rest/station/sensors/{}'.format(SAMPLE_STATION_ID)
        server.send_response(request, text=SAMPLE_RESULT_STATION_SENSORS, content_type='application/json',)

        await task

        available_params = station.get_available_params()

        assert set(available_params)==set(('C6H6','CO', 'NO2', 'O3', 'PM10', 'PM2.5', 'SO2'))

@pytest.mark.asyncio
async def test_station_async_fetch_measurements(aiohttp_redirector, loop):
    async with CaseControlledTestServer() as server:
        aiohttp_redirector.add_server('api.gios.gov.pl', 80, server.port)
        api = asynchronous.API(session=aiohttp_redirector.session)

        station = asynchronous.StationAsync(
            api=api,
            station_id=SAMPLE_STATION_ID,
            name='Warszawa-Komunikacyjna',
            city='Warszawa',
            lon='21.004724',
            lat='52.219298',
            timeout=5,
            distance=3)

        assert station.name == 'Warszawa-Komunikacyjna'

        task = loop.create_task(station.async_fetch_sensors_data())
        request = await server.receive_request()

        assert request.path_qs == '/pjp-api/rest/station/sensors/{}'.format(SAMPLE_STATION_ID)
        server.send_response(request, text=SAMPLE_RESULT_STATION_SENSORS_SMALL, content_type='application/json',)

        await task

        available_params = station.get_available_params()
        assert set(available_params)==set(('C6H6', 'PM10', 'PM2.5'))

        task = loop.create_task(station.async_fetch_measurements())
        for x in range(1):
            request = await server.receive_request()
            
            assert request.path_qs == '/pjp-api/rest/data/getData/3575'
            server.send_response(request, text=SAMPLE_RESULT_STATION_DATA_3575, content_type='application/json',)
            server.send_response(request, text=SAMPLE_RESULT_STATION_DATA_3575, content_type='application/json',)
            server.send_response(request, text=SAMPLE_RESULT_STATION_DATA_3575, content_type='application/json',)
            await task

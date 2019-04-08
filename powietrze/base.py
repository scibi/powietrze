# -*- coding: utf-8 -*-

import math


class BaseAPI:
    def __init__(self, timeout=5):
        self._api_all_stations_url = 'http://api.gios.gov.pl/pjp-api/rest/station/findAll'
        self._stations_data = []

        self._timeout = timeout

    def create_stations_by_ids(self, station_ids):
        rv = []
        for station in self._stations_data:
            if station['id'] in station_ids:
                rv.append(self._create_station_object(station))
        return rv

    def create_stations_by_names(self, names):
        rv = []
        for station in self._stations_data:
            if station['stationName'] in names:
                rv.append(self._create_station_object(station))
        return rv

    def create_stations_by_city_name(self, city_name):
        rv = []
        for station in self._stations_data:
            if station['city'] is not None and station['city']['name'] == city_name:
                rv.append(self._create_station_object(station))
        return rv

    def create_stations_by_location(self, latitude, longitude, max_stations=5, max_distance=30000):
        data = [(self._approximate_distance(
            (latitude, longitude),
            (float(s['gegrLat']), float(s['gegrLon']))), s) for s in self._stations_data]
        data.sort(key=lambda x: x[0])
        return [self._create_station_object(station, dist)
                for dist, station in (data[:max_stations]) if dist <= max_distance]

    def _approximate_distance(self, point1, point2):
        R = 6372800  # Earth radius in meters
        lat1, lon1 = point1
        lat2, lon2 = point2
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + \
            math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
        return 2*R*math.atan2(math.sqrt(a), math.sqrt(1 - a))

    def _create_station_object(self, class_name, station, distance=None):
        return class_name(
                station_id=station['id'],
                name=station['stationName'],
                city=station['city']['name'],
                lon=station['gegrLon'],
                lat=station['gegrLat'],
                timeout=self._timeout,
                distance=distance)


class BaseStation:
    def __init__(self, station_id, name, city, lon, lat, timeout=5, distance=None):
        self._station_id = station_id
        self._api_station_url = 'http://api.gios.gov.pl/pjp-api/rest/station/sensors/{stationId}'
        self._api_sensor_url = 'http://api.gios.gov.pl/pjp-api/rest/data/getData/{sensorId}'

        self._sensors_data = []
        self._measurements = {}
        self._timeout = timeout

        self._name = name
        self._city = city
        self._lon = lon
        self._lat = lat
        self._distance = distance

    def __str__(self):
        if self._distance is None:
            return "{} [{}/{}]".format(self._name, self._station_id, self._city)
        else:
            return "{} [{}/{} - {:.1f} km]".format(self._name, self._station_id, self._city,
                                                   self._distance/1000)

    def __repr__(self):
        return self.__str__()

    @property
    def name(self):
        return self._name

    def _get_sensors_ids(self):
        return [sensor['id'] for sensor in self._sensors_data]

    def get_newest_measurements(self):
        rv = {}
        for key, value in self._measurements.items():
            for measurement in value:
                if measurement['value'] is not None:
                    rv[key] = measurement
                    break
        return rv

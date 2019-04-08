# -*- coding: utf-8 -*-

"""Console script for powietrze."""
import sys
import click

import sync


def print_station(station) -> None:
    station.fetch_sensors_data()
    station.fetch_measurements()

    click.echo('{}:'.format(station))
    measurements = station.get_newest_measurements()
    for key, value in measurements.items():
        click.echo('    {:5} {:7.2f} μg/m³ [{}]'.format(key, value['value'], value['date']))


@click.group()
def cli():
    pass


@cli.command()
@click.argument('station_ids', type=int, nargs=-1)
def get_measurements_by_station_id(station_ids):
    click.echo('Get newest measurements by station IDs ({})'.format(station_ids))
    click.echo()

    api = sync.API()
    api.fetch_stations_data()
    stations = api.create_stations_by_ids(station_ids)

    for station in stations:
        print_station(station)


@cli.command()
@click.argument('station_names', nargs=-1)
def get_measurements_by_station_name(station_names):
    click.echo('Get newest measurements by station names ({})'.format(station_names))
    click.echo()

    api = sync.API()
    api.fetch_stations_data()
    stations = api.create_stations_by_names(station_names)

    for station in stations:
        print_station(station)


@cli.command()
@click.argument('city_name')
def get_measurements_by_city_name(city_name):
    click.echo('Get newest measurements by city name ({})'.format(city_name))
    click.echo()

    api = sync.API()
    api.fetch_stations_data()
    stations = api.create_stations_by_city_name(city_name)

    for station in stations:
        print_station(station)


@cli.command()
@click.argument('latitude', type=float)
@click.argument('longitude', type=float)
@click.option('--max_stations', default=3, show_default=True,
              help='Maximum number of stations to return.')
@click.option('--max_distance', default=10, show_default=True, type=float,
              help='Maximum distance to station in kilometers.')
def get_measurements_by_location(latitude, longitude, max_stations, max_distance):
    click.echo('Get newest measurements by location ({}, {})'.format(latitude, longitude))
    click.echo('max_stations = {}'.format(max_stations))
    click.echo('max_distance = {} km'.format(max_distance))
    click.echo()

    api = sync.API()
    api.fetch_stations_data()
    stations = api.create_stations_by_location(latitude, longitude, max_stations=max_stations,
                                               max_distance=max_distance*1000)

    for station in stations:
        print_station(station)


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover

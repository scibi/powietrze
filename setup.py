#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = ['Click>=6.0', 'requests>=2.21.0', 'aiohttp>=3.5.4']

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author="Patryk Ściborek",
    author_email='patryk@sciborek.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description="Python library to talk to the http://powietrze.gios.gov.pl API.",
    entry_points={
        'console_scripts': [
            'powietrze=powietrze.cli:cli',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='powietrze',
    name='powietrze',
    packages=find_packages(include=['powietrze']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/scibi/powietrze',
    version='0.1.0',
    zip_safe=False,
)

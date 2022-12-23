from setuptools import setup
from os.path import join, dirname

with open(join(dirname(__file__), 'openaddr', 'VERSION')) as file:
    version = file.read().strip()

setup(
    name = 'batch-machine',
    version = version,
    url = 'https://github.com/openaddresses/batch-machine',
    author = 'Michal Migurski',
    author_email = 'mike-pypi@teczno.com',
    description = 'In-progress scripts for running OpenAddresses on a complete data set and publishing the results.',
    packages = ['openaddr', 'openaddr.util', 'openaddr.tests'],
    entry_points = dict(
        console_scripts = [
            'openaddr-preview-source = openaddr.preview:main',
            'openaddr-process-one = openaddr.process_one:main',
        ]
    ),
    package_data = {
        'openaddr': [
            'VERSION'
        ],
        'openaddr.tests': [
            'data/*.*', 'outputs/*.*', 'sources/*.*', 'sources/fr/*.*',
            'sources/us/*/*.*', 'sources/de/*.*', 'sources/nl/*.*',
            'sources/be/*/*.json', 'conforms/lake-man-gdb.gdb/*',
            'conforms/*.csv', 'conforms/*.dbf', 'conforms/*.zip', 'conforms/*.gfs',
            'conforms/*.gml', 'conforms/*.json', 'conforms/*.prj', 'conforms/*.shp',
            'conforms/*.shx', 'conforms/*.vrt'
        ]
    },
    test_suite = 'openaddr.tests',
    install_requires = [
        'dateutils == 0.6.12', 'ijson == 2.4',

        'simplejson == 3.18.0',

        # http://www.voidspace.org.uk/python/mock/
        'mock == 3.0.5',

        # https://github.com/uri-templates/uritemplate-py/
        'uritemplate == 3.0.0',

        # http://docs.python-requests.org/en/master/
        'requests == 2.28.1',

        # https://github.com/patrys/httmock
        'httmock == 1.4.0',

        # https://github.com/openaddresses/pyesridump
        'esridump == 1.11.0',

        # Used in openaddr.parcels
        'Shapely == 2.0.0',

        # https://github.com/tilezen/mapbox-vector-tile
        'mapbox-vector-tile @ git+https://github.com/tilezen/mapbox-vector-tile.git@5249a23da696515f118564dbcc83b39216792454#egg=mapbox-vector-tile',
        'future==0.18.2',
        'protobuf==4.21',
    ]
)

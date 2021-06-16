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
            'geodata/*.shp', 'geodata/*.shx', 'geodata/*.prj', 'geodata/*.dbf',
            'geodata/*.cpg', 'VERSION',
        ],
        'openaddr.tests': [
            'data/*.*', 'outputs/*.*', 'sources/*.*', 'sources/fr/*.*',
            'sources/us/*/*.*', 'sources/de/*.*', 'sources/nl/*.*',
            'sources/be/*/*.json', 'conforms/lake-man-gdb.gdb/*',
            'conforms/*.csv', 'conforms/*.dbf', 'conforms/*.zip', 'conforms/*.gfs',
            'conforms/*.gml', 'conforms/*.json', 'conforms/*.prj', 'conforms/*.shp',
            'conforms/*.shx', 'conforms/*.vrt',
            'parcels/sources/us/ca/*.*', 'parcels/sources/us/id/*.*',
            'parcels/data/*.*', 'parcels/data/us/ca/*.*',
            'parcels/data/us/ca/berkeley/*.*'
        ],
        'openaddr.util': [
            'templates/*.*'
        ]
    },
    test_suite = 'openaddr.tests',
    install_requires = [
        'boto == 2.49.0', 'dateutils == 0.6.6', 'ijson == 2.4',

        'simplejson == 3.17.2',

        # http://www.voidspace.org.uk/python/mock/
        'mock == 3.0.5',

        # https://github.com/uri-templates/uritemplate-py/
        'uritemplate == 3.0.0',

        # http://docs.python-requests.org/en/master/
        'requests == 2.22.0',

        # https://github.com/patrys/httmock
        'httmock == 1.3.0',

        # https://boto3.readthedocs.org
        'boto3 == 1.11.5',

        # https://github.com/openaddresses/pyesridump
        'esridump == 1.10.1',

        # Used in openaddr.parcels
        'Shapely == 1.7.1',

        # Used in dotmaps preview to support S3-backed SQLite mbtiles
        # https://rogerbinns.github.io/apsw/
        'apsw == 3.9.2.post1',

        # http://pythonhosted.org/itsdangerous/
        'itsdangerous == 1.1.0',

        # https://github.com/tilezen/mapbox-vector-tile
        'mapbox-vector-tile==1.2.0',
        'future==0.16.0',
        'protobuf==3.5.1',
        ]
)

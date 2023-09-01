# coding=utf8
"""
Run Python test suite via the standard unittest mechanism.
Usage:
  python test.py
  python test.py --logall
  python test.py TestConformTransforms
  python test.py -l TestOA.test_process
All logging is suppressed unless --logall or -l specified
~/.openaddr-logging-test.json can also be used to configure log behavior
"""


from __future__ import absolute_import, division, print_function

import unittest
import shutil
import tempfile
import json
import re
import pickle
import sys
import os
import csv
import logging
from os import close, environ, mkdir, remove
from io import BytesIO
from csv import DictReader
from itertools import cycle
from zipfile import ZipFile
from datetime import datetime, timedelta
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qs
from os.path import dirname, join, basename, exists, splitext
from contextlib import contextmanager
from subprocess import Popen, PIPE
from unicodedata import normalize
from threading import Lock

if sys.platform != 'win32':
    from fcntl import lockf, LOCK_EX, LOCK_UN
else:
    lockf, LOCK_EX, LOCK_UN = None, None, None

from requests import get
from httmock import response, HTTMock
import mock

from .. import cache, conform, process_one
from ..cache import CacheResult
from ..conform import ConformResult
from ..process_one import find_source_problem, SourceProblem

" Return an x,y array given a wkt point string"
def wkt_pt(pt_str):
    pt = pt_str.strip().replace('POINT', '').replace('(', '').replace(')', '').strip().split(' ')
    return float(pt[0]), float(pt[1])

def touch_first_arg_file(path, *args, **kwargs):
    ''' Write a short dummy file for the first argument.
    '''
    with open(path, 'w') as file:
        file.write('yo')

def touch_second_arg_file(_, path, *args, **kwargs):
    ''' Write a short dummy file for the second argument.
    '''
    with open(path, 'w') as file:
        file.write('yo')

def return_path_in_second_arg_dir(_, path, *args, **kwargs):
    ''' Write a short dummy file inside the directory specified in the first arg and return its path.
    '''
    with open(os.path.join(path, "out.geojson"), 'w') as file:
        file.write('yo')
    return os.path.join(path, "out.geojson")

class TestOA (unittest.TestCase):

    def setUp(self):
        ''' Prepare a clean temporary directory, and copy sources there.
        '''
        self.testdir = tempfile.mkdtemp(prefix='testOA-')
        self.src_dir = join(self.testdir, 'sources')
        sources_dir = join(dirname(__file__), 'sources')
        shutil.copytree(sources_dir, self.src_dir)

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        data_dirname = join(dirname(__file__), 'data')
        local_path = None

        if host == 'fake-s3.local':
            return response(200, self.s3._read_fake_key(path))

        if (host, path) == ('data.acgov.org', '/api/geospatial/8e4s-7f4v'):
            local_path = join(data_dirname, 'us-ca-alameda_county-excerpt.zip')

        if (host, path) == ('data.acgov.org', '/api/geospatial/MiXeD-cAsE'):
            local_path = join(data_dirname, 'us-ca-alameda_county-excerpt-mixedcase.zip')

        if (host, path) == ('www.ci.berkeley.ca.us', '/uploadedFiles/IT/GIS/Parcels.zip'):
            local_path = join(data_dirname, 'us-ca-berkeley-excerpt.zip')

        if (host, path) == ('www.ci.berkeley.ca.us', '/uploadedFiles/IT/GIS/No-Parcels.zip'):
            return response(404, 'Nobody here but us coats')

        if (host, path) == ('www.dropbox.com', '/s/fhopgbg4vkyoobr/czech_addresses_wgs84_12092016_MASTER.zip'):
            return response(404, 'Nobody here but us coats')

        if (host, path) == ('data.openaddresses.io', '/cache/uploads/migurski/d5add2/oregon_state_addresses.zip'):
            return response(404, 'Nobody here but us coats')

        if (host, path) == ('data.openoakland.org', '/sites/default/files/OakParcelsGeo2013_0.zip'):
            local_path = join(data_dirname, 'us-ca-oakland-excerpt.zip')

        if (host, path) == ('data.openaddresses.io', '/cache/pl.zip'):
            local_path = join(data_dirname, 'pl.zip')

        if (host, path) == ('data.openaddresses.io', '/cache/jp-fukushima.zip'):
            local_path = join(data_dirname, 'jp-fukushima.zip')

        if (host, path) == ('data.sfgov.org', '/download/kvej-w5kb/ZIPPED%20SHAPEFILE'):
            local_path = join(data_dirname, 'us-ca-san_francisco-excerpt.zip')

        if (host, path) == ('ftp.vgingis.com', '/Download/VA_SiteAddress.txt.zip'):
            local_path = join(data_dirname, 'VA_SiteAddress-excerpt.zip')

        if (host, path) == ('gis3.oit.ohio.gov', '/LBRS/_downloads/TRU_ADDS.zip'):
            local_path = join(data_dirname, 'TRU_ADDS-excerpt.zip')

        if (host, path) == ('data.openaddresses.io', '/cache/uploads/iandees/ed482f/bucks.geojson.zip'):
            local_path = join(data_dirname, 'us-pa-bucks.geojson.zip')

        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnIdsOnly') == ['true']:
                local_path = join(data_dirname, 'us-ca-carson-ids-only.json')
            elif qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-ca-carson-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-ca-carson-0.json')

        if (host, path) == ('www.carsonproperty.info', '/ArcGIS/rest/services/basemap/MapServer/1'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-ca-carson-metadata.json')

        if (host, path) == ('72.205.198.131', '/ArcGIS/rest/services/Brown/Brown/MapServer/33/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnIdsOnly') == ['true']:
                local_path = join(data_dirname, 'us-ks-brown-ids-only.json')
            elif qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-ks-brown-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-ks-brown-0.json')

        if (host, path) == ('72.205.198.131', '/ArcGIS/rest/services/Brown/Brown/MapServer/33'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-ks-brown-metadata.json')

        if (host, path) == ('services1.arcgis.com', '/I6XnrlnguPDoEObn/arcgis/rest/services/AddressPoints/FeatureServer/0/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-pa-lancaster-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-pa-lancaster-0.json')
            elif body_data.get('resultRecordCount') == ['1']:
                local_path = join(data_dirname, 'us-pa-lancaster-probe.json')

        if (host, path) == ('services1.arcgis.com', '/I6XnrlnguPDoEObn/arcgis/rest/services/AddressPoints/FeatureServer/0'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-pa-lancaster-metadata.json')

        if (host, path) == ('services.geoportalmaps.com', '/arcgis/rest/services/Runnels_Services/MapServer/1'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-tx-runnels-metadata.json')

        if (host, path) == ('maps.co.washington.mn.us', '/arcgis/rest/services/Public/Public_Parcels/MapServer/0/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-nm-washington-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-nm-washington-0.json')
            elif body_data.get('resultRecordCount') == ['1']:
                local_path = join(data_dirname, 'us-nm-washington-probe.json')

        if (host, path) == ('maps.co.washington.mn.us', '/arcgis/rest/services/Public/Public_Parcels/MapServer/0'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-nm-washington-metadata.json')

        if (host, path) == ('gis.ci.waco.tx.us', '/arcgis/rest/services/Parcels/MapServer/0/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-tx-waco-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-tx-waco-0.json')

        if (host, path) == ('gis.ci.waco.tx.us', '/arcgis/rest/services/Parcels/MapServer/0'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-tx-waco-metadata.json')

        if (host, path) == ('ocgis.orangecountygov.com', '/ArcGIS/rest/services/Dynamic/LandBase/MapServer/0/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnIdsOnly') == ['true']:
                local_path = join(data_dirname, 'us-ny-orange-ids-only.json')
            elif qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-ny-orange-count-only.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'us-ny-orange-0.json')

        if (host, path) == ('ocgis.orangecountygov.com', '/ArcGIS/rest/services/Dynamic/LandBase/MapServer/0'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-ny-orange-metadata.json')

        if (host, path) == ('cdr.citynet.kharkov.ua', '/arcgis/rest/services/gis_ort_stat_general/MapServer/1/query'):
            qs = parse_qs(query)
            body_data = parse_qs(request.body) if request.body else {}

            if qs.get('returnIdsOnly') == ['true']:
                local_path = join(data_dirname, 'ua-kharkiv-ids-only.json')
            if qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'ua-kharkiv-count-only.json')
            elif 'outStatistics' in qs:
                local_path = join(data_dirname, 'ua-kharkiv-statistics.json')
            elif body_data.get('outSR') == ['4326']:
                local_path = join(data_dirname, 'ua-kharkiv-0.json')

        if (host, path) == ('cdr.citynet.kharkov.ua', '/arcgis/rest/services/gis_ort_stat_general/MapServer/1'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'ua-kharkiv-metadata.json')

        if (host, path) == ('data.openaddresses.io', '/20000101/us-ca-carson-cached.json'):
            local_path = join(data_dirname, 'us-ca-carson-cache.geojson')

        if (host, path) == ('data.openaddresses.io', '/cache/fr/BAN_licence_gratuite_repartage_75.zip'):
            local_path = join(data_dirname, 'BAN_licence_gratuite_repartage_75.zip')

        if (host, path) == ('data.openaddresses.io', '/cache/fr/BAN_licence_gratuite_repartage_974.zip'):
            local_path = join(data_dirname, 'BAN_licence_gratuite_repartage_974.zip')

        if (host, path) == ('fbarc.stadt-berlin.de', '/FIS_Broker_Atom/Hauskoordinaten/HKO_EPSG3068.zip'):
            local_path = join(data_dirname, 'de-berlin-excerpt.zip')

        if (host, path) == ('www.dropbox.com', '/s/8uaqry2w657p44n/bagadres.zip'):
            local_path = join(data_dirname, 'nl.zip')

        if (host, path) == ('s.irisnet.be', '/v1/AUTH_b4e6bcc3-db61-442e-8b59-e0ce9142d182/Region/UrbAdm_SHP.zip'):
            local_path = join(data_dirname, 'be-wa-brussels.zip')

        if (host, path) == ('data.openaddresses.io', '/cache/uploads/migurski/ed789f/toscana20160804.zip'):
            local_path = join(data_dirname, 'it-52-statewide.zip')

        if (host, path) == ('data.openaddresses.io', '/cache/uploads/nvkelso/5a5bf6/ParkCountyADDRESS_POINTS_point.zip'):
            local_path = join(data_dirname, 'us-wy-park.zip')

        if (host, path) == ('njgin.state.nj.us', '/download2/Address/ADDR_POINT_NJ_fgdb.zip'):
            local_path = join(data_dirname, 'nj-statewide.gdb.zip')

        if (host, path) == ('data.openaddresses.io', '/cache/uploads/trescube/f5df2e/us-mi-grand-traverse.geojson.zip'):
            local_path = join(data_dirname, 'us-mi-grand-traverse.geojson.zip')

        if (host, path) == ('fake-web', '/lake-man.gdb.zip'):
            local_path = join(data_dirname, 'lake-man.gdb.zip')

        if (host, path) == ('fake-web', '/lake-man-gdb-othername.zip'):
            local_path = join(data_dirname, 'lake-man-gdb-othername.zip')

        if (host, path) == ('fake-web', '/lake-man-gdb-othername-nodir.zip'):
            local_path = join(data_dirname, 'lake-man-gdb-othername-nodir.zip')

        if scheme == 'file':
            local_path = path

        if local_path:
            type, _ = guess_type(local_path)
            with open(local_path, 'rb') as file:
                return response(200, file.read(), headers={'Content-Type': type})

        raise NotImplementedError(url.geturl())

    def response_content_ftp(self, url):
        ''' Fake FTP responses for use with mock.patch in tests.
        '''
        scheme, host, path, _, _, _ = urlparse(url)
        data_dirname = join(dirname(__file__), 'data')
        local_path = None

        if scheme != 'ftp':
            raise ValueError("Don't know how to {}".format(scheme))

        if (host, path) == ('ftp.agrc.utah.gov', '/UtahSGID_Vector/UTM12_NAD83/LOCATION/UnpackagedData/AddressPoints/_Statewide/AddressPoints_shp.zip'):
            local_path = join(data_dirname, 'us-ut-excerpt.zip')

        if (host, path) == ('ftp02.portlandoregon.gov', '/CivicApps/address.zip'):
            local_path = join(data_dirname, 'us-or-portland.zip')

        if (host, path) == ('ftp.skra.is', '/skra/STADFANG.dsv.zip'):
            local_path = join(data_dirname, 'iceland.zip')

        if local_path:
            type, _ = guess_type(local_path)
            with open(local_path, 'rb') as file:
                return response(200, file.read(), headers={'Content-Type': type})

        raise NotImplementedError(url)

    def test_single_ac_local(self):
        ''' Test complete process_one.process on Alameda County sample data with a local filepath
        '''
        data_dirname = join(dirname(__file__), 'data')
        local_path = join(data_dirname, 'us-ca-alameda_county-excerpt.zip')
        shutil.copy(local_path, '/tmp/us-ca-alameda.zip')
        source = join(self.src_dir, 'us-ca-alameda_county-local.json')

        with HTTMock(self.response_content), \
             mock.patch('openaddr.preview.render') as preview_ren, \
             mock.patch('openaddr.slippymap.generate') as slippymap_gen, \
             mock.patch('openaddr.process_one.render_geojsonld') as render_geojsonld:
            preview_ren.side_effect = touch_second_arg_file
            slippymap_gen.side_effect = touch_first_arg_file
            render_geojsonld.side_effect = return_path_in_second_arg_dir
            state_path = process_one.process(source, self.testdir, "addresses", "default", True, True, True, True, mapbox_key='mapbox-XXXX')

        self.assertTrue(slippymap_gen.mock_calls[0][1][0].endswith('.mbtiles'))
        self.assertTrue(slippymap_gen.mock_calls[0][1][1].endswith('.geojson'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][0].endswith('.pmtiles'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][1].endswith('.geojson'))
        self.assertTrue(render_geojsonld.mock_calls[0][1][0].endswith('.csv'))

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['preview'])
        self.assertIsNotNone(state['slippymap'])
        self.assertIsNotNone(state['pmtiles'])
        self.assertIsNotNone(state['geojsonld'])

        output_path = join(dirname(state_path), state['processed'])
        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[1]['ID'], '')
            self.assertEqual(rows[10]['ID'], '')
            self.assertEqual(rows[100]['ID'], '')
            self.assertEqual(rows[1000]['ID'], '')
            self.assertEqual(rows[1]['NUMBER'], '2147')
            self.assertEqual(rows[10]['NUMBER'], '605')
            self.assertEqual(rows[100]['NUMBER'], '167')
            self.assertEqual(rows[1000]['NUMBER'], '322')
            self.assertEqual(rows[1]['STREET'], 'BROADWAY')
            self.assertEqual(rows[10]['STREET'], 'HILLSBOROUGH ST')
            self.assertEqual(rows[100]['STREET'], '8TH ST')
            self.assertEqual(rows[1000]['STREET'], 'HANOVER AV')
            self.assertEqual(rows[1]['UNIT'], '')
            self.assertEqual(rows[10]['UNIT'], '')
            self.assertEqual(rows[100]['UNIT'], '')
            self.assertEqual(rows[1000]['UNIT'], '')

    def test_single_ac(self):
        ''' Test complete process_one.process on Alameda County sample data.
        '''
        source = join(self.src_dir, 'us-ca-alameda_county.json')

        with HTTMock(self.response_content), \
             mock.patch('openaddr.preview.render') as preview_ren, \
             mock.patch('openaddr.slippymap.generate') as slippymap_gen, \
             mock.patch('openaddr.process_one.render_geojsonld') as render_geojsonld:
            preview_ren.side_effect = touch_second_arg_file
            slippymap_gen.side_effect = touch_first_arg_file
            render_geojsonld.side_effect = return_path_in_second_arg_dir
            state_path = process_one.process(source, self.testdir, "addresses", "default", True, True, True, True, mapbox_key='mapbox-XXXX')

        self.assertTrue(slippymap_gen.mock_calls[0][1][0].endswith('.mbtiles'))
        self.assertTrue(slippymap_gen.mock_calls[0][1][1].endswith('.geojson'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][0].endswith('.pmtiles'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][1].endswith('.geojson'))
        self.assertTrue(render_geojsonld.mock_calls[0][1][0].endswith('.csv'))

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['preview'])
        self.assertIsNotNone(state['slippymap'])
        self.assertIsNotNone(state['pmtiles'])

        output_path = join(dirname(state_path), state['processed'])
        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[1]['ID'], '')
            self.assertEqual(rows[10]['ID'], '')
            self.assertEqual(rows[100]['ID'], '')
            self.assertEqual(rows[1000]['ID'], '')
            self.assertEqual(rows[1]['NUMBER'], '2147')
            self.assertEqual(rows[10]['NUMBER'], '605')
            self.assertEqual(rows[100]['NUMBER'], '167')
            self.assertEqual(rows[1000]['NUMBER'], '322')
            self.assertEqual(rows[1]['STREET'], 'BROADWAY')
            self.assertEqual(rows[10]['STREET'], 'HILLSBOROUGH ST')
            self.assertEqual(rows[100]['STREET'], '8TH ST')
            self.assertEqual(rows[1000]['STREET'], 'HANOVER AV')
            self.assertEqual(rows[1]['UNIT'], '')
            self.assertEqual(rows[10]['UNIT'], '')
            self.assertEqual(rows[100]['UNIT'], '')
            self.assertEqual(rows[1000]['UNIT'], '')

    def test_single_ac_mixedcase(self):
        ''' Test complete process_one.process on Alameda County sample data.
        '''
        source = join(self.src_dir, 'us-ca-alameda_county-mixedcase.json')

        with HTTMock(self.response_content), \
             mock.patch('openaddr.preview.render') as preview_ren, \
             mock.patch('openaddr.slippymap.generate') as slippymap_gen, \
             mock.patch('openaddr.process_one.render_geojsonld') as render_geojsonld:
            preview_ren.side_effect = touch_second_arg_file
            slippymap_gen.side_effect = touch_first_arg_file
            render_geojsonld.side_effect = return_path_in_second_arg_dir
            state_path = process_one.process(source, self.testdir, "addresses", "default", True, True, True, True, mapbox_key='mapbox-XXXX')

        self.assertTrue(slippymap_gen.mock_calls[0][1][0].endswith('.mbtiles'))
        self.assertTrue(slippymap_gen.mock_calls[0][1][1].endswith('.geojson'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][0].endswith('.pmtiles'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][1].endswith('.geojson'))
        self.assertTrue(render_geojsonld.mock_calls[0][1][0].endswith('.csv'))

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['preview'])
        self.assertIsNotNone(state['slippymap'])
        self.assertIsNotNone(state['pmtiles'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[1]['ID'], '')
            self.assertEqual(rows[10]['ID'], '')
            self.assertEqual(rows[100]['ID'], '')
            self.assertEqual(rows[1000]['ID'], '')
            self.assertEqual(rows[1]['NUMBER'], '2147')
            self.assertEqual(rows[10]['NUMBER'], '605')
            self.assertEqual(rows[100]['NUMBER'], '167')
            self.assertEqual(rows[1000]['NUMBER'], '322')
            self.assertEqual(rows[1]['STREET'], 'BROADWAY')
            self.assertEqual(rows[10]['STREET'], 'HILLSBOROUGH ST')
            self.assertEqual(rows[100]['STREET'], '8TH ST')
            self.assertEqual(rows[1000]['STREET'], 'HANOVER AV')

    def test_single_sf(self):
        ''' Test complete process_one.process on San Francisco sample data.
        '''
        source = join(self.src_dir, 'us-ca-san_francisco.json')

        with HTTMock(self.response_content), \
             mock.patch('openaddr.preview.render') as preview_ren, \
             mock.patch('openaddr.slippymap.generate') as slippymap_gen, \
             mock.patch('openaddr.process_one.render_geojsonld') as render_geojsonld:
            preview_ren.side_effect = touch_second_arg_file
            slippymap_gen.side_effect = touch_first_arg_file
            render_geojsonld.side_effect = return_path_in_second_arg_dir
            state_path = process_one.process(source, self.testdir, "addresses", "default", True, True, True, True, mapbox_key='mapbox-XXXX')

        self.assertTrue(slippymap_gen.mock_calls[0][1][0].endswith('.mbtiles'))
        self.assertTrue(slippymap_gen.mock_calls[0][1][1].endswith('.geojson'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][0].endswith('.pmtiles'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][1].endswith('.geojson'))
        self.assertTrue(render_geojsonld.mock_calls[0][1][0].endswith('.csv'))

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['preview'])
        self.assertIsNotNone(state['slippymap'])
        self.assertIsNotNone(state['pmtiles'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[1]['ID'], '')
            self.assertEqual(rows[10]['ID'], '')
            self.assertEqual(rows[100]['ID'], '')
            self.assertEqual(rows[1000]['ID'], '')
            self.assertEqual(rows[1]['NUMBER'], '27')
            self.assertEqual(rows[10]['NUMBER'], '42')
            self.assertEqual(rows[100]['NUMBER'], '209')
            self.assertEqual(rows[1000]['NUMBER'], '1415')
            self.assertEqual(rows[1]['STREET'], 'OCTAVIA ST')
            self.assertEqual(rows[10]['STREET'], 'GOLDEN GATE AVE')
            self.assertEqual(rows[100]['STREET'], 'OCTAVIA ST')
            self.assertEqual(rows[1000]['STREET'], 'FOLSOM ST')
            self.assertEqual(rows[1]['UNIT'], '')
            self.assertEqual(rows[10]['UNIT'], '')
            self.assertEqual(rows[100]['UNIT'], '')
            self.assertEqual(rows[1000]['UNIT'], '')

    def test_single_car(self):
        ''' Test complete process_one.process on Carson sample data.
        '''
        source = join(self.src_dir, 'us-ca-carson.json')

        with HTTMock(self.response_content), \
             mock.patch('openaddr.preview.render') as preview_ren, \
             mock.patch('openaddr.slippymap.generate') as slippymap_gen, \
             mock.patch('openaddr.process_one.render_geojsonld') as render_geojsonld:
            preview_ren.side_effect = touch_second_arg_file
            slippymap_gen.side_effect = touch_first_arg_file
            render_geojsonld.side_effect = return_path_in_second_arg_dir
            state_path = process_one.process(source, self.testdir, "addresses", "default", True, True, True, True, mapbox_key='mapbox-XXXX')

        self.assertTrue(slippymap_gen.mock_calls[0][1][0].endswith('.mbtiles'))
        self.assertTrue(slippymap_gen.mock_calls[0][1][1].endswith('.geojson'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][0].endswith('.pmtiles'))
        self.assertTrue(slippymap_gen.mock_calls[1][1][1].endswith('.geojson'))
        self.assertTrue(render_geojsonld.mock_calls[0][1][0].endswith('.csv'))

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertEqual(state['fingerprint'], '23082fe4819682a6934b61443560160c')
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['preview'])
        self.assertIsNotNone(state['slippymap'])
        self.assertIsNotNone(state['pmtiles'])

        with open(join(dirname(state_path), state['processed'])) as file:
            rows = list(DictReader(file, dialect='excel'))
            self.assertEqual(5, len(rows))
            self.assertEqual(rows[0]['NUMBER'], '555')
            self.assertEqual(rows[0]['STREET'], 'CARSON ST')
            self.assertEqual(rows[0]['UNIT'], '')
            self.assertEqual(rows[0]['CITY'], 'CARSON, CA')
            self.assertEqual(rows[0]['POSTCODE'], '90745')
            self.assertEqual(rows[0]['DISTRICT'], '')
            self.assertEqual(rows[0]['REGION'], '')
            self.assertEqual(rows[0]['ID'], '')

    def test_single_car_cached(self):
        ''' Test complete process_one.process on Carson sample data.
        '''
        source = join(self.src_dir, 'us-ca-carson-cached.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertEqual(state['fingerprint'], '1821b2e50a61ed04ac2213fbc7a1984d')
        self.assertIsNotNone(state['processed'])
        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        with open(join(dirname(state_path), state['processed'])) as file:
            self.assertTrue('555,CARSON ST' in file.read())

    def test_single_car_old_cached(self):
        ''' Test complete process_one.process on Carson sample data.
        '''
        source = join(self.src_dir, 'us-ca-carson-old-cached.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertEqual(state['fingerprint'], '1821b2e50a61ed04ac2213fbc7a1984d')
        self.assertIsNotNone(state['processed'])
        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        with open(join(dirname(state_path), state['processed'])) as file:
            self.assertTrue('555,CARSON ST' in file.read())

    def test_single_tx_runnels(self):
        ''' Test complete process_one.process on Oakland sample data.
        '''
        source = join(self.src_dir, 'us/tx/runnels.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['cache'])
        self.assertIsNone(state['processed'])
        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        # This test data does not contain a working conform object
        self.assertEqual(state['source problem'], "Missing required ESRI token")

    def test_single_oak(self):
        ''' Test complete process_one.process on Oakland sample data.
        '''
        source = join(self.src_dir, 'us-ca-oakland.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertFalse(state['skipped'])
        self.assertIsNotNone(state['cache'])
        # This test data does not contain a working conform object
        self.assertEqual(state['source problem'], "Unknown source conform format")
        self.assertIsNone(state["processed"])
        self.assertIsNone(state["preview"])
        self.assertIsNone(state["slippymap"])

    def test_single_oak_skip(self):
        ''' Test complete process_one.process on Oakland sample data.
        '''
        source = join(self.src_dir, 'us-ca-oakland-skip.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        # This test data says "skip": True
        self.assertEqual(state["source problem"], "Source says to skip")
        self.assertTrue(state["skipped"])
        self.assertIsNone(state["cache"])
        self.assertIsNone(state["processed"])
        self.assertIsNone(state["preview"])
        self.assertIsNone(state["slippymap"])

    def test_single_berk(self):
        ''' Test complete process_one.process on Berkeley sample data.
        '''
        source = join(self.src_dir, 'us-ca-berkeley.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state["cache"])
        # This test data does not contain a conform object at all
        self.assertEqual(state["source problem"], "Source is missing a conform object")
        self.assertIsNone(state["processed"])
        self.assertIsNone(state["preview"])
        self.assertIsNone(state["slippymap"])


    def test_single_berk_404(self):
        ''' Test complete process_one.process on 404 sample data.
        '''
        source = join(self.src_dir, 'us-ca-berkeley-404.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertEqual(state["source problem"], "Could not download source data")
        self.assertIsNone(state["cache"])
        self.assertIsNone(state["processed"])
        self.assertIsNone(state["preview"])
        self.assertIsNone(state["slippymap"])

    def test_single_berk_apn(self):
        ''' Test complete process_one.process on Berkeley sample data.
        '''
        source = join(self.src_dir, 'us-ca-berkeley-apn.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[1]['ID'], '055 188300600')
            self.assertEqual(rows[10]['ID'], '055 189504000')
            self.assertEqual(rows[100]['ID'], '055 188700100')
            self.assertEqual(rows[1]['NUMBER'], '2418')
            self.assertEqual(rows[10]['NUMBER'], '2029')
            self.assertEqual(rows[100]['NUMBER'], '2298')
            self.assertEqual(rows[1]['STREET'], 'DANA ST')
            self.assertEqual(rows[10]['STREET'], 'CHANNING WAY')
            self.assertEqual(rows[100]['STREET'], 'DURANT AVE')
            self.assertEqual(rows[1]['UNIT'], u'')
            self.assertEqual(rows[10]['UNIT'], u'')
            self.assertEqual(rows[100]['UNIT'], u'')

    def test_single_pl_ds(self):
        ''' Test complete process_one.process on Polish sample data.
        '''
        source = join(self.src_dir, 'pl-dolnoslaskie.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

    def test_single_pl_l(self):
        ''' Test complete process_one.process on Polish sample data.
        '''
        source = join(self.src_dir, 'pl-lodzkie.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state['cache'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[1]['NUMBER'], u'5')
            self.assertEqual(rows[10]['NUMBER'], u'8')
            self.assertEqual(rows[100]['NUMBER'], u'5a')
            self.assertEqual(rows[1]['STREET'], u'Ulica Dolnych Wa\u0142\xf3w  Gliwice')
            self.assertEqual(rows[10]['STREET'], u'Ulica Dolnych Wa\u0142\xf3w  Gliwice')
            self.assertEqual(rows[100]['STREET'], u'Plac pl. Inwalid\xf3w Wojennych  Gliwice')
            self.assertEqual(rows[1]['UNIT'], u'')
            self.assertEqual(rows[10]['UNIT'], u'')
            self.assertEqual(rows[100]['UNIT'], u'')

    def test_single_jp_fukushima2(self):
        ''' Test complete process_one.process on Japanese sample data.
        '''
        source = join(self.src_dir, 'jp-fukushima2.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state["source problem"])
        self.assertIsNotNone(state["processed"])
        self.assertIsNone(state["preview"])
        self.assertIsNone(state["slippymap"])

        with open(join(dirname(state_path), state["processed"]), encoding='utf8') as file:
            rows = list(csv.DictReader(file))

        self.assertEqual(len(rows), 6)
        self.assertEqual(rows[0]['NUMBER'], u'24-9')
        self.assertEqual(rows[0]['STREET'], u'田沢字姥懐')
        self.assertEqual(rows[1]['NUMBER'], u'16-9')
        self.assertEqual(rows[1]['STREET'], u'田沢字躑躅ケ森')
        self.assertEqual(rows[2]['NUMBER'], u'22-9')
        self.assertEqual(rows[2]['STREET'], u'小田字正夫田')
        self.assertEqual(rows[0]['GEOM'], 'POINT (140.480007 37.706391)')
        self.assertEqual(rows[1]['GEOM'], 'POINT (140.486267 37.707664)')
        self.assertEqual(rows[2]['GEOM'], 'POINT (140.41875 37.710239)')

    def test_single_utah(self):
        ''' Test complete process_one.process on data that uses file selection with mixed case (issue #104)
        '''
        source = join(self.src_dir, 'us-ut.json')

        with mock.patch('openaddr.util.request_ftp_file', new=self.response_content_ftp):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

    def test_single_iceland(self):
        ''' Test complete process_one.process.
        '''
        source = join(self.src_dir, 'iceland.json')

        with mock.patch('openaddr.util.request_ftp_file', new=self.response_content_ftp):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])
        self.assertIsNotNone(state['processed'])
        self.assertIsNotNone(state['cache'])

        with open(join(dirname(state_path), state['processed']), encoding='utf8') as file:
            rows = list(csv.DictReader(file))

        self.assertEqual(len(rows), 15)
        self.assertEqual(rows[0]['STREET'], u'2.Gata v/Rauðavatn')
        self.assertEqual(rows[2]['GEOM'], 'POINT (-21.7684622 64.110974)')
        self.assertEqual(rows[3]['GEOM'], 'POINT (-21.7665982 64.1100444)')

    def test_single_fr_paris(self):
        ''' Test complete process_one.process on data that uses conform csvsplit (issue #124)
        '''
        source = join(self.src_dir, 'fr-paris.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

    def test_single_fr_lareunion(self):
        ''' Test complete process_one.process on data that uses non-UTF8 encoding (issue #136)
        '''
        source = None

        for form in ('NFC', 'NFD'):
            normalized = normalize(form, u'fr/la-réunion.json')
            if os.path.exists(join(self.src_dir, normalized)):
                source = join(self.src_dir, normalized)
                break

        if source is None:
            raise Exception('Could not find a usable fr/la-réunion.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

    def test_single_va_statewide(self):
        ''' Test complete process_one.process on data with non-OGR .csv filename.
        '''
        source = join(self.src_dir, 'us/va/statewide.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])


    def test_single_oh_trumbull(self):
        ''' Test complete process_one.process on data with .txt filename present.
        '''
        source = join(self.src_dir, 'us/oh/trumbull.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])


    def test_single_ks_brown(self):
        ''' Test complete process_one.process on data with ESRI multiPolyline geometries.
        '''
        source = join(self.src_dir, 'us/ks/brown_county.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])


    def test_single_pa_lancaster(self):
        ''' Test complete process_one.process on data with ESRI multiPolyline geometries.
        '''
        source = join(self.src_dir, 'us/pa/lancaster.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[1]['UNIT'], u'2')
            self.assertEqual(rows[11]['UNIT'], u'11')
            self.assertEqual(rows[21]['UNIT'], u'')
            self.assertEqual(rows[1]['NUMBER'], u'423')
            self.assertEqual(rows[11]['NUMBER'], u'423')
            self.assertEqual(rows[21]['NUMBER'], u'7')
            self.assertEqual(rows[1]['STREET'], u'W 28TH DIVISION HWY')
            self.assertEqual(rows[11]['STREET'], u'W 28TH DIVISION HWY')
            self.assertEqual(rows[21]['STREET'], u'W 28TH DIVISION HWY')

    def test_single_ua_kharkiv(self):
        ''' Test complete process_one.process on data with ESRI multiPolyline geometries.
        '''
        source = join(self.src_dir, 'ua-63-city_of_kharkiv.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

    def test_single_pa_bucks(self):
        ''' Test complete process_one.process on data with ESRI multiPolyline geometries.
        '''
        source = join(self.src_dir, 'us/pa/bucks.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[1]['UNIT'], u'')
            self.assertEqual(rows[10]['UNIT'], u'')
            self.assertEqual(rows[20]['UNIT'], u'')
            self.assertEqual(rows[1]['NUMBER'], u'')
            self.assertEqual(rows[10]['NUMBER'], u'')
            self.assertEqual(rows[20]['NUMBER'], u'429')
            self.assertEqual(rows[1]['STREET'], u'STATE RD')
            self.assertEqual(rows[10]['STREET'], u'STATE RD')
            self.assertEqual(rows[20]['STREET'], u'WALNUT AVE E')

    def test_single_nm_washington(self):
        ''' Test complete process_one.process on data without ESRI support for resultRecordCount.
        '''
        source = join(self.src_dir, 'us/nm/washington.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[1]['UNIT'], u'')
            self.assertEqual(rows[5]['UNIT'], u'')
            self.assertEqual(rows[9]['UNIT'], u'')
            self.assertEqual(rows[1]['NUMBER'], u'9884')
            self.assertEqual(rows[5]['NUMBER'], u'3842')
            self.assertEqual(rows[9]['NUMBER'], u'')
            self.assertEqual(rows[1]['STREET'], u'5TH STREET LN N')
            self.assertEqual(rows[5]['STREET'], u'ABERCROMBIE LN')
            self.assertEqual(rows[9]['STREET'], u'')

    def test_single_tx_waco(self):
        ''' Test complete process_one.process on data without ESRI support for resultRecordCount.
        '''
        source = join(self.src_dir, 'us/tx/city_of_waco.json')

        with HTTMock(self.response_content):
            ofs = csv.field_size_limit()
            csv.field_size_limit(1)
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)
            csv.field_size_limit(ofs)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertEqual(state["source problem"], "Could not conform source data")
        self.assertIsNone(state["processed"])

        source = join(self.src_dir, 'us/tx/city_of_waco.json')

        with HTTMock(self.response_content):
            ofs = csv.field_size_limit()
            csv.field_size_limit(sys.maxsize)
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)
            csv.field_size_limit(ofs)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state["source problem"])
        self.assertIsNotNone(state["processed"])
        self.assertIsNone(state["preview"])
        self.assertIsNone(state["slippymap"])

        output_path = join(dirname(state_path), state["processed"])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[0]['REGION'], u'TX')
            self.assertEqual(rows[0]['ID'], u'')
            self.assertEqual(rows[0]['NUMBER'], u'308')
            self.assertEqual(rows[0]['HASH'], u'c782a47f7d52d99f')
            self.assertEqual(rows[0]['CITY'], u'Mcgregor')
            self.assertEqual(rows[0]['GEOM'], u'POINT (-97.3961768 31.4432706)')
            self.assertEqual(rows[0]['STREET'], u'PULLEN ST')
            self.assertEqual(rows[0]['POSTCODE'], u'76657')
            self.assertEqual(rows[0]['UNIT'], u'')
            self.assertEqual(rows[0]['DISTRICT'], u'')

    def test_single_wy_park(self):
        ''' Test complete process_one.process on data without ESRI support for resultRecordCount.
        '''
        source = join(self.src_dir, 'us-wy-park.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state["processed"])

        output_path = join(dirname(state_path), state["processed"])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[0]['ID'], u'')
            self.assertEqual(rows[0]['NUMBER'], u'162')
            self.assertEqual(rows[0]['HASH'], u'fa774c4d6e199cb1')
            self.assertEqual(rows[0]['CITY'], u'')
            self.assertEqual(rows[0]['GEOM'], u'POINT (-108.7563613 44.7538737)')
            self.assertEqual(rows[0]['STREET'], u'N CLARK ST')
            self.assertEqual(rows[0]['POSTCODE'], u'')
            self.assertEqual(rows[0]['UNIT'], u'')
            self.assertEqual(rows[0]['DISTRICT'], u'')

    def test_single_ny_orange(self):
        ''' Test complete process_one.process on data NaN values in ESRI response.
        '''
        source = join(self.src_dir, 'us-ny-orange.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNotNone(state["processed"])

        output_path = join(dirname(state_path), state["processed"])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[0]['ID'], u'')
            self.assertEqual(rows[0]['NUMBER'], u'434')
            self.assertEqual(rows[0]['HASH'], u'58a4d4fbbf126d86')
            self.assertEqual(rows[0]['CITY'], u'MONROE')
            self.assertEqual(rows[0]['GEOM'], u'POINT (-74.1926686 41.3187728)')
            self.assertEqual(rows[0]['STREET'], u'')
            self.assertEqual(rows[0]['POSTCODE'], u'10950')
            self.assertEqual(rows[0]['UNIT'], u'')
            self.assertEqual(rows[0]['DISTRICT'], u'')

    def test_single_de_berlin(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'de/berlin.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(rows[0]['NUMBER'], u'72')
            self.assertEqual(rows[1]['NUMBER'], u'3')
            self.assertEqual(rows[2]['NUMBER'], u'75')
            self.assertEqual(rows[0]['STREET'], u'Otto-Braun-Stra\xdfe')
            self.assertEqual(rows[1]['STREET'], u'Dorotheenstra\xdfe')
            self.assertEqual(rows[2]['STREET'], u'Alte Jakobstra\xdfe')

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

    def test_single_us_or_portland(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'us/or/portland.json')

        with mock.patch('openaddr.util.request_ftp_file', new=self.response_content_ftp):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(len(rows), 12)
            self.assertEqual(rows[2]['NUMBER'], u'1')
            self.assertEqual(rows[3]['NUMBER'], u'10')
            self.assertEqual(rows[-2]['NUMBER'], u'2211')
            self.assertEqual(rows[-1]['NUMBER'], u'2211')
            self.assertEqual(rows[2]['STREET'], u'SW RICHARDSON ST')
            self.assertEqual(rows[3]['STREET'], u'SW PORTER ST')
            self.assertEqual(rows[-2]['STREET'], u'SE OCHOCO ST')
            self.assertEqual(rows[-1]['STREET'], u'SE OCHOCO ST')
            self.assertTrue(bool(rows[2]['GEOM']))
            self.assertTrue(bool(rows[3]['GEOM']))
            self.assertFalse(bool(rows[-2]['GEOM']))
            self.assertTrue(bool(rows[-1]['GEOM']))

    def test_single_nl_countrywide(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'nl/countrywide.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(len(rows), 8)
            self.assertEqual(rows[0]['NUMBER'], u'34x')
            self.assertEqual(rows[1]['NUMBER'], u'65-x')
            self.assertEqual(rows[2]['NUMBER'], u'147x-x')
            self.assertEqual(rows[3]['NUMBER'], u'6')
            self.assertEqual(rows[4]['NUMBER'], u'279b')
            self.assertEqual(rows[5]['NUMBER'], u'10')
            self.assertEqual(rows[6]['NUMBER'], u'601')
            self.assertEqual(rows[7]['NUMBER'], u'2')

    def test_single_be_wa_brussels(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'be/wa/brussels-fr.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(len(rows), 666)
            self.assertEqual(rows[0]['NUMBER'], u'2')
            self.assertEqual(rows[0]['STREET'], u'Rue de la Victoire')
            self.assertEqual(rows[1]['NUMBER'], u'16')
            self.assertEqual(rows[1]['STREET'], u'Rue Fontainas')
            self.assertEqual(rows[2]['NUMBER'], u'23C')
            self.assertEqual(rows[2]['STREET'], u'Rue Fontainas')
            self.assertEqual(rows[3]['NUMBER'], u'2')
            self.assertEqual(rows[3]['STREET'], u"Rue de l'Eglise Saint-Gilles")

            x,y = wkt_pt(rows[0]['GEOM'])
            self.assertAlmostEqual(4.3458216, x, places=4)
            self.assertAlmostEqual(50.8324706, y, places=4)

            x,y = wkt_pt(rows[1]['GEOM'])
            self.assertAlmostEqual(4.3412631, x, places=4)
            self.assertAlmostEqual(50.8330868, y, places=4)

            x,y = wkt_pt(rows[2]['GEOM'])
            self.assertAlmostEqual(4.3410663, x, places=4)
            self.assertAlmostEqual(50.8334315, y, places=4)

            x,y = wkt_pt(rows[3]['GEOM'])
            self.assertAlmostEqual(4.3421632, x, places=4)
            self.assertAlmostEqual(50.8322201, y, places=4)

    def test_single_it_52_statewide(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'it-52-statewide.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(len(rows), 19)
            self.assertEqual(rows[0]['NUMBER'], u'33')
            self.assertEqual(rows[0]['STREET'], u'VIA CARLO CARRÀ')
            self.assertEqual(rows[1]['NUMBER'], u'23')
            self.assertEqual(rows[1]['STREET'], u'VIA CARLO CARRÀ')
            self.assertEqual(rows[2]['NUMBER'], u'2')
            self.assertEqual(rows[2]['STREET'], u'VIA MARINO MARINI')
            self.assertEqual(rows[0]['GEOM'], 'POINT (10.1863188 43.9562646)')
            self.assertEqual(rows[1]['GEOM'], 'POINT (10.1856048 43.9558156)')
            self.assertEqual(rows[2]['GEOM'], 'POINT (10.1860548 43.9553626)')

    def test_single_us_nj_statewide(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'us/nj/statewide.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(len(rows), 1045)
            self.assertEqual(rows[0]['NUMBER'], u'7')
            self.assertEqual(rows[0]['STREET'], u'Sagamore Avenue')
            self.assertEqual(rows[1]['NUMBER'], u'29')
            self.assertEqual(rows[1]['STREET'], u'Sagamore Avenue')
            self.assertEqual(rows[2]['NUMBER'], u'47')
            self.assertEqual(rows[2]['STREET'], u'Seneca Place')
            self.assertEqual(rows[0]['GEOM'], 'POINT (-74.0012016 40.3201199)')
            self.assertEqual(rows[1]['GEOM'], 'POINT (-74.0027904 40.3203365)')
            self.assertEqual(rows[2]['GEOM'], 'POINT (-74.0011386 40.3166497)')

    def test_single_cz_countrywide(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'cz-countrywide-bad-tests.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIs(state["tests passed"], False)
        self.assertIsNone(state["processed"])
        self.assertEqual(state["source problem"], "An acceptance test failed")

    def test_single_or_curry(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'us-or-curry.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertTrue(state["tests passed"])
        self.assertIsNone(state["processed"])
        self.assertEqual(state["source problem"], "Could not download source data")

    def test_single_mi_grand_traverse(self):
        '''
        '''
        source = join(self.src_dir, 'us-mi-grand_traverse.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state["processed"])
        print(state)
        self.assertEqual(state["source problem"], "Found no features in source data")

    def test_single_lake_man_gdb(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'lake-man-gdb.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(len(rows), 6)
            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[1]['NUMBER'], '5121')
            self.assertEqual(rows[1]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[2]['NUMBER'], '5133')
            self.assertEqual(rows[2]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[3]['NUMBER'], '5126')
            self.assertEqual(rows[3]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[4]['NUMBER'], '5120')
            self.assertEqual(rows[4]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[5]['NUMBER'], '5115')
            self.assertEqual(rows[5]['STREET'], 'OLD MILL RD')

    def test_single_lake_man_gdb_nested(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'lake-man-gdb-nested.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(len(rows), 6)
            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[1]['NUMBER'], '5121')
            self.assertEqual(rows[1]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[2]['NUMBER'], '5133')
            self.assertEqual(rows[2]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[3]['NUMBER'], '5126')
            self.assertEqual(rows[3]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[4]['NUMBER'], '5120')
            self.assertEqual(rows[4]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[5]['NUMBER'], '5115')
            self.assertEqual(rows[5]['STREET'], 'OLD MILL RD')

    def test_single_lake_man_gdb_nested_nodir(self):
        ''' Test complete process_one.process on data.
        '''
        source = join(self.src_dir, 'lake-man-gdb-nested-nodir.json')

        with HTTMock(self.response_content):
            state_path = process_one.process(source, self.testdir, "addresses", "default", False, False, False, False)

        with open(state_path) as file:
            state = dict(zip(*json.load(file)))

        self.assertIsNone(state['preview'])
        self.assertIsNone(state['slippymap'])

        output_path = join(dirname(state_path), state['processed'])

        with open(output_path, encoding='utf8') as input:
            rows = list(csv.DictReader(input))
            self.assertEqual(len(rows), 6)
            self.assertEqual(rows[0]['NUMBER'], '5115')
            self.assertEqual(rows[0]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[1]['NUMBER'], '5121')
            self.assertEqual(rows[1]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[2]['NUMBER'], '5133')
            self.assertEqual(rows[2]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[3]['NUMBER'], '5126')
            self.assertEqual(rows[3]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[4]['NUMBER'], '5120')
            self.assertEqual(rows[4]['STREET'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[5]['NUMBER'], '5115')
            self.assertEqual(rows[5]['STREET'], 'OLD MILL RD')

class TestState (unittest.TestCase):

    def setUp(self):
        '''
        '''
        self.output_dir = tempfile.mkdtemp(prefix='TestState-')

    def tearDown(self):
        '''
        '''
        shutil.rmtree(self.output_dir)

    def test_write_state(self):
        '''
        '''
        log_handler = mock.Mock()

        with open(join(self.output_dir, 'log-handler-stream.txt'), 'w') as file:
            log_handler.stream.name = file.name

        with open(join(self.output_dir, 'processed.zip'), 'w') as file:
            processed_path = file.name

        with open(join(self.output_dir, 'preview.png'), 'w') as file:
            preview_path = file.name

        with open(join(self.output_dir, 'slippymap.mbtiles'), 'w') as file:
            mbtiles_path = file.name

        with open(join(self.output_dir, 'slippymap.pmtiles'), 'w') as file:
            pmtiles_path = file.name

        with open(join(self.output_dir, 'out.geojsonld'), 'w') as file:
            geojsonld_path = file.name

        conform_result = ConformResult(processed=None,
                                       feat_count=999,
                                       path=processed_path,
                                       elapsed=timedelta(seconds=1))

        cache_result = CacheResult(cache='http://example.com/cache.csv',
                                   fingerprint='ff9900', version='0.0.0',
                                   elapsed=timedelta(seconds=2))

        #
        # Check result of process_one.write_state().
        #
        args = dict(source='sources/foo.json', layer='addresses',
                    data_source_name='open-data', skipped=False,
                    destination=self.output_dir, log_handler=log_handler,
                    cache_result=cache_result, conform_result=conform_result,
                    temp_dir=self.output_dir, preview_path=preview_path,
                    mbtiles_path=mbtiles_path, pmtiles_path=pmtiles_path,
                    geojsonld_path=geojsonld_path,
                    tests_passed=True)

        path1 = process_one.write_state(**args)

        with open(path1) as file:
            state1 = dict(zip(*json.load(file)))

        self.assertEqual(state1['source'], 'foo.json')
        self.assertEqual(state1['skipped'], False)
        self.assertEqual(state1['cache'], 'http://example.com/cache.csv')
        self.assertEqual(state1['feat count'], 999)
        self.assertEqual(state1['version'], '0.0.0')
        self.assertEqual(state1['fingerprint'], 'ff9900')
        self.assertEqual(state1['cache time'], '0:00:02')
        self.assertEqual(state1['processed'], 'out.zip')
        self.assertEqual(state1['process time'], '0:00:01')
        self.assertEqual(state1['output'], 'output.txt')
        self.assertEqual(state1['preview'], 'preview.png')
        self.assertEqual(state1['slippymap'], 'slippymap.mbtiles')
        self.assertEqual(state1['geojsonld'], 'out.geojsonld')
        self.assertEqual(state1['tests passed'], True)

        #
        # Tweak a few values, try process_one.write_state() again.
        #

        args.update(source='sources/foo/bar.json', skipped=True)
        path2 = process_one.write_state(**args)

        with open(path2) as file:
            state2 = dict(zip(*json.load(file)))

        self.assertEqual(state2['source'], 'bar.json')
        self.assertEqual(state2['skipped'], True)

    def test_find_source_problem(self):
        '''
        '''
        self.assertIsNone({'source problem': find_source_problem('', {'coverage': {'US Census': None}})}["source problem"])
        self.assertIsNone({'source problem': find_source_problem('', {'coverage': {'US Census': None}})}["source problem"])
        self.assertIsNone({'source problem': find_source_problem('', {'coverage': {'ISO 3166': None}})}["source problem"])

        self.assertIs({'source problem': find_source_problem('', {})}["source problem"], SourceProblem.no_coverage)
        self.assertIs({'source problem': find_source_problem('WARNING: Could not download ESRI source data: Could not retrieve layer metadata: Token Required', {})}["source problem"], SourceProblem.no_esri_token)
        self.assertIs({'source problem': find_source_problem('WARNING: Error doing conform; skipping', {})}["source problem"], SourceProblem.conform_source_failed)
        self.assertIs({'source problem': find_source_problem('WARNING: Could not download source data', {})}["source problem"], SourceProblem.download_source_failed)
        self.assertIs({'source problem': find_source_problem('WARNING: Unknown source conform protocol', {})}["source problem"], SourceProblem.unknown_conform_protocol)
        self.assertIs({'source problem': find_source_problem('WARNING: Unknown source conform format', {})}["source problem"], SourceProblem.unknown_conform_format)
        self.assertIs({'source problem': find_source_problem('WARNING: Unknown source conform type', {})}["source problem"], SourceProblem.unknown_conform_type)
        self.assertIs({'source problem': find_source_problem('WARNING: A source test failed', {})}["source problem"], SourceProblem.test_failed)
        self.assertIs({'source problem': find_source_problem('WARNING: Found no features in source data', {})}["source problem"], SourceProblem.no_features_found)


@contextmanager
def locked_open(filename):
    ''' Open and lock a file, for use with threads and processes.
    '''
    with open(filename, 'r+b') as file:
        if lockf:
            lockf(file, LOCK_EX)
        yield file
        if lockf:
            lockf(file, LOCK_UN)

class FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code

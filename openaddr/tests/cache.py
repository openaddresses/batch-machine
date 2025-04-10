from __future__ import absolute_import, division, print_function

import csv

from .. import SourceConfig
from urllib.parse import urlparse, parse_qs
from os.path import join, dirname

import json
import shutil
import mimetypes

from unittest.mock import patch
from esridump.errors import EsriDownloadError
import unittest
import httmock
import tempfile

from ..cache import guess_url_file_extension, EsriRestDownloadTask

class TestCacheExtensionGuessing (unittest.TestCase):

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        tests_dirname = dirname(__file__)

        if host == 'fake-cwd.local':
            with open(tests_dirname + path, 'rb') as file:
                type, _ = mimetypes.guess_type(file.name)
                return httmock.response(200, file.read(), headers={'Content-Type': type})

        elif (host, path) == ('www.ci.berkeley.ca.us', '/uploadedFiles/IT/GIS/Parcels.zip'):
            with open(join(tests_dirname, 'data', 'us-ca-berkeley-excerpt.zip'), 'rb') as file:
                return httmock.response(200, file.read(), headers={'Content-Type': 'application/octet-stream'})

        elif (host, path) == ('data.sfgov.org', '/download/kvej-w5kb/ZIPPED%20SHAPEFILE'):
            return httmock.response(302, '', headers={'Location': 'http://apps.sfgov.org/datafiles/view.php?file=sfgis/eas_addresses_with_units.zip'})

        elif (host, path, query) == ('apps.sfgov.org', '/datafiles/view.php', 'file=sfgis/eas_addresses_with_units.zip'):
            with open(join(tests_dirname, 'data', 'us-ca-san_francisco-excerpt.zip'), 'rb') as file:
                return httmock.response(200, file.read(), headers={'Content-Type': 'application/download', 'Content-Disposition': 'attachment; filename=eas_addresses_with_units.zip;'})

        elif (host, path, query) == ('dcatlas.dcgis.dc.gov', '/catalog/download.asp', 'downloadID=2182&downloadTYPE=ESRI'):
            return httmock.response(200, b'FAKE'*99, headers={'Content-Type': 'application/x-zip-compressed'})

        elif (host, path, query) == ('data.northcowichan.ca', '/DataBrowser/DownloadCsv', 'container=mncowichan&entitySet=PropertyReport&filter=NOFILTER'):
            return httmock.response(200, b'FAKE,FAKE\n'*99, headers={'Content-Type': 'text/csv', 'Content-Disposition': 'attachment; filename=PropertyReport.csv'})

        raise NotImplementedError(url.geturl())

    def test_urls(self):
        with httmock.HTTMock(self.response_content):
            assert guess_url_file_extension('http://fake-cwd.local/conforms/lake-man-3740.csv') == '.csv'
            assert guess_url_file_extension('http://fake-cwd.local/data/us-ca-carson-0.json') == '.json'
            assert guess_url_file_extension('http://fake-cwd.local/data/us-ca-oakland-excerpt.zip') == '.zip'
            assert guess_url_file_extension('http://www.ci.berkeley.ca.us/uploadedFiles/IT/GIS/Parcels.zip') == '.zip'
            assert guess_url_file_extension('https://data.sfgov.org/download/kvej-w5kb/ZIPPED%20SHAPEFILE') == '.zip'
            assert guess_url_file_extension('http://dcatlas.dcgis.dc.gov/catalog/download.asp?downloadID=2182&downloadTYPE=ESRI') == '.zip'
            assert guess_url_file_extension('http://data.northcowichan.ca/DataBrowser/DownloadCsv?container=mncowichan&entitySet=PropertyReport&filter=NOFILTER') == '.csv', guess_url_file_extension('http://data.northcowichan.ca/DataBrowser/DownloadCsv?container=mncowichan&entitySet=PropertyReport&filter=NOFILTER')

class TestCacheEsriDownload (unittest.TestCase):

    def setUp(self):
        ''' Prepare a clean temporary directory, and work there.
        '''
        self.workdir = tempfile.mkdtemp(prefix='testCache-')

    def tearDown(self):
        shutil.rmtree(self.workdir)

    def response_content(self, url, request):
        ''' Fake HTTP responses for use with HTTMock in tests.
        '''
        scheme, host, path, _, query, _ = urlparse(url.geturl())
        tests_dirname = dirname(__file__)
        data_dirname = join(tests_dirname, 'data')
        local_path = None

        if (host, path) == ('web2.kcsgis.com', '/kcsgis/rest/services/Cullman/VAM_Cullman_FS/FeatureServer/4'):
            qs = parse_qs(query)

            if qs.get('f') == ['json']:
                local_path = join(data_dirname, 'us-al-cullman-metadata.json')

        if (host, path) == ('web2.kcsgis.com', '/kcsgis/rest/services/Cullman/VAM_Cullman_FS/FeatureServer/4/query'):
            qs = parse_qs(query)
            body_qs = parse_qs(request.body)

            if qs.get('returnCountOnly') == ['true']:
                local_path = join(data_dirname, 'us-al-cullman-count-only.json')
            if request.method == 'POST' and body_qs.get('resultOffset') == ['0']:
                local_path = join(data_dirname, 'us-al-cullman-0.json')

        if local_path:
            type, _ = mimetypes.guess_type(local_path)
            with open(local_path, 'rb') as file:
                return httmock.response(200, file.read(), headers={'Content-Type': type})

        raise NotImplementedError(url.geturl())

    def test_download_with_conform(self):
        """ ESRI Caching Will Request With The Minimum Fields Required """
        conforms = (
            (None, None),
            (['a', 'b', 'c'], {'type': 'csv', 'street': ['a', 'b'], 'number': 'c'}),
            (['a'], {'type': 'csv', 'street': {'function': 'regexp', 'field': 'a'}, 'number': {'function': 'regexp', 'field': 'a'}}),
        )

        task = EsriRestDownloadTask('us-fl-palmbeach')
        for expected, conform in conforms:
            c = SourceConfig(dict({
                "schema": 2,
                "layers": {
                    "addresses": [{
                        "name": "default",
                        "conform": conform
                    }]
                }
            }), "addresses", "default")
            actual = task.field_names_to_request(c)
            self.assertEqual(expected, actual)

    def test_download_handles_no_count(self):
        """ ESRI Caching Will Handle A Server Without returnCountOnly Support """
        task = EsriRestDownloadTask('us-fl-palmbeach')

        with patch('esridump.EsriDumper.get_metadata') as metadata_patch:
            metadata_patch.return_value = {'fields': []}
            with patch('esridump.EsriDumper.get_feature_count') as feature_patch:
                feature_patch.side_effect = EsriDownloadError("Server doesn't support returnCountOnly")
                with self.assertRaises(EsriDownloadError) as e:
                    task.download(['http://example.com/'], self.workdir, SourceConfig(dict({
                        "schema": 2,
                        "layers": {
                            "addresses": [{
                                "name": "default",
                                "conform": {
                                    "number": "num",
                                    "street": "str"
                                }
                            }]
                        }
                    }), "addresses", "default"))

                    # This is the expected exception at this point
                    self.assertEqual(e.message, "Could not find object ID field name for deduplication")

    def test_field_names_to_request(self):
        '''
        '''
        conform1 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": "Number",
                        "street": "Street"
                    }
                }]
            }
        }), "addresses", "default")

        fields1 = EsriRestDownloadTask.field_names_to_request(conform1)
        self.assertEqual(fields1, ['Number', 'Street'])

        conform2 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": "Number",
                        "street": {
                            "function": "regexp",
                            "field": "Street"
                        }
                    }
                }]
            }
        }), "addresses", "default")
        fields2 = EsriRestDownloadTask.field_names_to_request(conform2)
        self.assertEqual(fields2, ['Number', 'Street'])

        conform3 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": "Number",
                        "street": {
                            "function": "prefixed_number",
                            "field": "Street"
                        }
                    }
                }]
            }
        }), "addresses", "default")
        fields3 = EsriRestDownloadTask.field_names_to_request(conform3)
        self.assertEqual(fields3, ['Number', 'Street'])

        conform4 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": "Number",
                        "street": {
                            "function": "postfixed_street",
                            "field": "Street"
                        }
                    }
                }]
            }
        }), "addresses", "default")
        fields4 = EsriRestDownloadTask.field_names_to_request(conform4)
        self.assertEqual(fields4, ['Number', 'Street'])

        conform5 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": "Number",
                        "street": {
                            "function": "remove_prefix",
                            "field": "Street"
                        }
                    }
                }]
            }
        }), "addresses", "default")
        fields5 = EsriRestDownloadTask.field_names_to_request(conform5)
        self.assertEqual(fields5, ['Number', 'Street'])

        conform6 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": "Number",
                        "street": {
                            "function": "remove_postfix",
                            "field": "Street"
                        }
                    }
                }]
            }
        }), "addresses", "default")
        fields6 = EsriRestDownloadTask.field_names_to_request(conform6)
        self.assertEqual(fields6, ['Number', 'Street'])

        conform7 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "street": {
                            "function": "join",
                            "fields": ["Number", "Street"]
                        }
                    }
                }]
            }
        }), "addresses", "default")
        fields7 = EsriRestDownloadTask.field_names_to_request(conform7)
        self.assertEqual(fields7, ['Number', 'Street'])

        conform8 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "street": {
                            "function": "format",
                            "fields": ["Number", "Street"]
                        }
                    }
                }]
            }
        }), "addresses", "default")
        fields8 = EsriRestDownloadTask.field_names_to_request(conform8)
        self.assertEqual(fields8, ['Number', 'Street'])

        conform9 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "street": ["Number", "Street"]
                    }
                }]
            }
        }), "addresses", "default")
        fields9 = EsriRestDownloadTask.field_names_to_request(conform9)
        self.assertEqual(fields9, ['Number', 'Street'])

        conform10 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "street": {
                            "function": "chain",
                            "variable": "foo",
                            "functions": [{
                                "function": "postfixed_street",
                                "field": "Street"
                            },{
                                "function": "remove_postfix",
                                "field": "foo"
                            }]
                        }
                    }
                }]
            }
        }), "addresses", "default")
        fields10 = EsriRestDownloadTask.field_names_to_request(conform10)
        self.assertEqual(fields10, ['Street'])

        conform11 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "street": ["Number", "Street"],
                        "number": {
                            "function": "constant",
                            "value": "123",
                        },
                        "state": "StateColumn",
                    }
                }]
            }
        }), "addresses", "default")
        fields11 = EsriRestDownloadTask.field_names_to_request(conform11)
        self.assertEqual(fields11, ['Number', 'Street', 'StateColumn'])

    def test_handle_feature_server_with_lat_lon_in_conform(self):
        '''
        '''
        task = EsriRestDownloadTask('us-fl-palmbeach')
        c = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "lat": "LAT",
                        "lon": "LON"
                    }
                }]
            }
        }), "addresses", "default")
        with httmock.HTTMock(self.response_content):
            output_path = task.download(["https://web2.kcsgis.com/kcsgis/rest/services/Cullman/VAM_Cullman_FS/FeatureServer/4"], self.workdir, c)
            self.assertEqual(len(output_path), 1)

            # Load the downloaded CSV and check the geometry
            with open(output_path[0], 'r') as file:
                reader = csv.DictReader(file)
                all_data = list(reader)
                self.assertEqual(len(all_data),  5)
                self.assertTrue('oa:geom' in all_data[0])
                self.assertEqual(all_data[0]['oa:geom'], 'POINT (-86.82960553 34.18671398)')

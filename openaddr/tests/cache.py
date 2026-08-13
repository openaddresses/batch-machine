from __future__ import absolute_import, division, print_function

import csv

from .. import SourceConfig
from .. import cache as cache_fn
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

import sys
from ..cache import guess_url_file_extension, EsriRestDownloadTask, URLDownloadTask, DownloadTask

# openaddr/__init__.py defines a `cache` function that shadows the `cache`
# submodule on the `openaddr` package object, so `from .. import cache`
# would grab the function, not the module. Go through sys.modules instead.
cache_module = sys.modules['openaddr.cache']

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
                        "region": "StateColumn",
                    }
                }]
            }
        }), "addresses", "default")
        fields11 = EsriRestDownloadTask.field_names_to_request(conform11)
        self.assertEqual(fields11, ['Number', 'StateColumn', 'Street'])

        # Test that integer accuracy values are skipped (not treated as field names)
        # The 'accuracy' field can be an integer constant indicating a fixed accuracy level
        conform12 = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "street": "StreetName",
                        "number": "HouseNumber",
                        "accuracy": 1,
                    }
                }]
            }
        }), "addresses", "default")
        fields12 = EsriRestDownloadTask.field_names_to_request(conform12)
        self.assertEqual(fields12, ['HouseNumber', 'StreetName'])

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

class TestFromProtocolStringHeaders (unittest.TestCase):

    def test_headers_reach_url_download_task(self):
        task = DownloadTask.from_protocol_string('http', 'us-il-champaign', headers={'Referer': 'https://example.gov/'})
        self.assertIsInstance(task, URLDownloadTask)
        self.assertEqual(task.headers['Referer'], 'https://example.gov/')
        # The default User-Agent is still present alongside custom headers.
        self.assertIn('User-Agent', task.headers)

    def test_headers_reach_esri_download_task(self):
        task = DownloadTask.from_protocol_string('ESRI', 'us-il-champaign', headers={'Referer': 'https://example.gov/'})
        self.assertIsInstance(task, EsriRestDownloadTask)
        self.assertEqual(task.headers['Referer'], 'https://example.gov/')

    def test_headers_reach_esri_dumper(self):
        ''' EsriRestDownloadTask.download() must pass its headers to
            EsriDumper as extra_headers, since pyesridump makes its own
            requests independent of openaddr.cache.request().
        '''
        workdir = tempfile.mkdtemp(prefix='testCacheHeaders-')
        try:
            task = EsriRestDownloadTask('us-fl-palmbeach', headers={'Referer': 'https://example.gov/'})
            c = SourceConfig(dict({
                "schema": 2,
                "layers": {
                    "addresses": [{
                        "name": "default",
                        "conform": None
                    }]
                }
            }), "addresses", "default")

            with patch.object(cache_module, 'EsriDumper') as dumper_patch:
                dumper_patch.return_value.get_metadata.return_value = {'fields': []}
                dumper_patch.return_value.get_feature_count.return_value = 0
                dumper_patch.return_value.__iter__.return_value = iter([])

                task.download(['http://example.com/'], workdir, c)

                _, kwargs = dumper_patch.call_args
                self.assertEqual(kwargs.get('extra_headers'), task.headers)
                self.assertEqual(kwargs['extra_headers']['Referer'], 'https://example.gov/')
        finally:
            shutil.rmtree(workdir)

    def test_no_headers_still_gets_default_user_agent(self):
        task = DownloadTask.from_protocol_string('http', 'us-il-champaign')
        self.assertEqual(list(task.headers.keys()), ['User-Agent'])

    def test_custom_user_agent_overrides_default(self):
        task = DownloadTask.from_protocol_string('http', 'us-il-champaign', headers={'User-Agent': 'custom-agent/1.0'})
        self.assertEqual(task.headers['User-Agent'], 'custom-agent/1.0')

class TestURLDownloadTaskHeaders (unittest.TestCase):
    ''' Confirm that a source's custom headers are sent on both the
        file-extension pre-flight request and the real download request.
    '''

    def setUp(self):
        self.workdir = tempfile.mkdtemp(prefix='testCacheHeaders-')
        self.seen_referers = []

    def tearDown(self):
        shutil.rmtree(self.workdir)

    def response_content(self, url, request):
        scheme, host, path, _, query, _ = urlparse(url.geturl())

        # A query string forces guess_url_file_extension() to make a
        # sniffing request instead of trusting the URL's extension,
        # so this URL exercises both the pre-flight and real download.
        if (host, path, query) == ('headers-test.local', '/addresses.csv', 'download=true'):
            self.seen_referers.append(request.headers.get('Referer'))
            return httmock.response(200, b'FAKE,FAKE\n', headers={'Content-Type': 'text/csv'})

        raise NotImplementedError(url.geturl())

    def test_headers_sent_on_preflight_and_download_requests(self):
        task = URLDownloadTask('us-il-champaign', headers={'Referer': 'https://example.gov/gis/'})
        with httmock.HTTMock(self.response_content):
            output_files = task.download(['http://headers-test.local/addresses.csv?download=true'], self.workdir, None)

        self.assertEqual(len(output_files), 1)
        # One request for the extension-guessing pre-flight, one for the real download.
        self.assertEqual(self.seen_referers, ['https://example.gov/gis/', 'https://example.gov/gis/'])

class TestCacheHttpRequestSettings (unittest.TestCase):
    ''' Confirm that openaddr.cache() reads headers from the nested
        http_request_settings.headers key in the source config, and that a
        source with no http_request_settings at all still works.
    '''

    def setUp(self):
        self.destdir = tempfile.mkdtemp(prefix='testCacheHttpSettings-')
        self.seen_referers = []

    def tearDown(self):
        shutil.rmtree(self.destdir)

    def response_content(self, url, request):
        scheme, host, path, _, query, _ = urlparse(url.geturl())

        # A query string forces guess_url_file_extension() to make a
        # sniffing request instead of trusting the URL's extension,
        # so this URL exercises both the pre-flight and real download.
        if (host, path, query) == ('http-request-settings-test.local', '/addresses.csv', 'download=true'):
            self.seen_referers.append(request.headers.get('Referer'))
            return httmock.response(200, b'FAKE,FAKE\n', headers={'Content-Type': 'text/csv'})

        raise NotImplementedError(url.geturl())

    def make_source_config(self, layersource_extra):
        return SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [dict({
                    "name": "default",
                    "protocol": "http",
                    "data": "http://http-request-settings-test.local/addresses.csv?download=true",
                }, **layersource_extra)]
            }
        }), "addresses", "default")

    def test_cache_reads_headers_from_http_request_settings(self):
        source_config = self.make_source_config({
            "http_request_settings": {
                "headers": {"Referer": "https://example.gov/gis/"}
            }
        })

        with httmock.HTTMock(self.response_content):
            cache_fn(source_config, self.destdir, {})

        self.assertEqual(self.seen_referers, ['https://example.gov/gis/', 'https://example.gov/gis/'])

    def test_cache_without_http_request_settings_sends_no_referer(self):
        source_config = self.make_source_config({})

        with httmock.HTTMock(self.response_content):
            cache_fn(source_config, self.destdir, {})

        self.assertEqual(self.seen_referers, [None, None])

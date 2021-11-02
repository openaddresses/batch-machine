# coding=utf8
# Test suite. This code could be in a separate file

from shutil import rmtree
from os.path import dirname, join
from datetime import datetime
from shlex import quote

import unittest, tempfile, json, io
from mimetypes import guess_type
from urllib.parse import urlparse, parse_qs
from httmock import HTTMock, response
from mock import Mock, patch

from .. import util, __version__

class TestUtilities (unittest.TestCase):

    def test_request_ftp_file(self):
        '''
        '''
        data_sources = [
            # Two working cases based on real data
            (join(dirname(__file__), 'data', 'us-or-portland.zip'), 'ftp://ftp02.portlandoregon.gov/CivicApps/address.zip'),
            (join(dirname(__file__), 'data', 'us-ut-excerpt.zip'), 'ftp://ftp.agrc.utah.gov/UtahSGID_Vector/UTM12_NAD83/LOCATION/UnpackagedData/AddressPoints/_Statewide/AddressPoints_shp.zip'),

            # Some additional special cases
            (None, 'ftp://ftp02.portlandoregon.gov/CivicApps/address-fake.zip'),
            (None, 'ftp://username:password@ftp02.portlandoregon.gov/CivicApps/address-fake.zip'),
            ]

        for (zip_path, ftp_url) in data_sources:
            parsed = urlparse(ftp_url)

            with patch('ftplib.FTP') as FTP:
                if zip_path is None:
                    zip_bytes = None
                else:
                    with open(zip_path, 'rb') as zip_file:
                        zip_bytes = zip_file.read()

                cb_file = io.BytesIO()
                FTP.return_value.retrbinary.side_effect = lambda cmd, cb: cb_file.write(zip_bytes)

                with patch('openaddr.util.build_request_ftp_file_callback') as build_request_ftp_file_callback:
                    build_request_ftp_file_callback.return_value = cb_file, None
                    resp = util.request_ftp_file(ftp_url)

                FTP.assert_called_once_with(parsed.hostname)
                FTP.return_value.login.assert_called_once_with(parsed.username, parsed.password)
                FTP.return_value.retrbinary.assert_called_once_with('RETR {}'.format(parsed.path), None)

                if zip_bytes is None:
                    self.assertEqual(resp.status_code, 400, 'Nothing to return means failure')
                else:
                    self.assertEqual(resp.status_code, 200)
                    self.assertEqual(resp.content, zip_bytes, 'Expected number of bytes')

    def test_log_current_usage(self):
        '''
        '''
        with patch('openaddr.util.get_pidlist') as get_pidlist, \
             patch('openaddr.util.get_cpu_times') as get_cpu_times, \
             patch('openaddr.util.get_diskio_bytes') as get_diskio_bytes, \
             patch('openaddr.util.get_network_bytes') as get_network_bytes, \
             patch('openaddr.util.get_memory_usage') as get_memory_usage:
            get_cpu_times.return_value = 1, 2, 3
            get_diskio_bytes.return_value = 4, 5
            get_network_bytes.return_value = 6, 7
            get_memory_usage.return_value = 8

            previous = util.log_current_usage(0, 0, 0, 0, 0, 0, 0, 0, 0)

        get_cpu_times.assert_called_once_with(get_pidlist.return_value)
        get_diskio_bytes.assert_called_once_with(get_pidlist.return_value)
        get_network_bytes.assert_called_once_with()
        get_memory_usage.assert_called_once_with(get_pidlist.return_value)

        self.assertEqual(previous[:7], (2, 3, 1, 4, 5, 6, 7))

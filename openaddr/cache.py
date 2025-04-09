from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.cache')

import os
import errno
import math
import mimetypes
import shutil
import re
import csv

from os import mkdir
from hashlib import md5
from os.path import join, basename, exists, abspath, splitext
from urllib.parse import urlparse
from subprocess import check_output
from tempfile import mkstemp
from hashlib import sha1
from shutil import move
from shapely.geometry import shape
from esridump import EsriDumper
from esridump.errors import EsriDownloadError

import requests

# HTTP timeout in seconds, used in various calls to requests.get() and requests.post()
_http_timeout = 180

from .conform import GEOM_FIELDNAME
from . import util

def mkdirsp(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def traverse(item):
    "Iterates over nested iterables"
    if isinstance(item, list):
        for i in item:
            for j in traverse(i):
                yield j
    else:
        yield item

def request(method, url, **kwargs):
    if urlparse(url).scheme == 'ftp':
        if method != 'GET':
            raise NotImplementedError("Don't know how to {} with {}".format(method, url))
        return util.request_ftp_file(url)

    try:
        _L.debug("Requesting %s with args %s", url, kwargs.get('params') or kwargs.get('data'))
        return requests.request(method, url, timeout=_http_timeout, **kwargs)
    except requests.exceptions.SSLError as e:
        _L.warning("Retrying %s without SSL verification", url)
        return requests.request(method, url, timeout=_http_timeout, verify=False, **kwargs)

class CacheResult:
    cache = None
    fingerprint = None
    version = None
    elapsed = None

    def __init__(self, cache, fingerprint, version, elapsed):
        self.cache = cache
        self.fingerprint = fingerprint
        self.version = version
        self.elapsed = elapsed

    @staticmethod
    def empty():
        return CacheResult(None, None, None, None)

    def todict(self):
        return dict(cache=self.cache, fingerprint=self.fingerprint, version=self.version)


def compare_cache_details(filepath, resultdir, data):
    ''' Compare cache file with known source data, return cache and fingerprint.

        Checks if fresh data is already cached, returns a new file path if not.
    '''
    if not exists(filepath):
        raise Exception('cached file {} is missing'.format(filepath))

    fingerprint = md5()

    with open(filepath, 'rb') as file:
        while chunk := file.read(8192):
            fingerprint.update(chunk)

    # Determine if anything needs to be done at all.
    if urlparse(data.get('cache', '')).scheme == 'http' and 'fingerprint' in data:
        if fingerprint.hexdigest() == data['fingerprint']:
            return data['cache'], data['fingerprint']

    cache_name = basename(filepath)

    if not exists(resultdir):
        mkdir(resultdir)

    move(filepath, join(resultdir, cache_name))
    data_cache = 'file://' + join(abspath(resultdir), cache_name)

    return data_cache, fingerprint.hexdigest()

class DownloadError(Exception):
    pass


class DownloadTask(object):

    def __init__(self, source_prefix, params={}, headers={}):
        '''

            params: Additional query parameters, used by EsriRestDownloadTask.
            headers: Additional HTTP headers.
        '''
        self.source_prefix = source_prefix
        self.headers = {
            'User-Agent': 'openaddresses-extract/1.0 (https://github.com/openaddresses/openaddresses)',
        }
        self.headers.update(dict(**headers))
        self.query_params = dict(**params)


    @classmethod
    def from_protocol_string(clz, protocol_string, source_prefix=None):
        if protocol_string.lower() == 'http':
            return URLDownloadTask(source_prefix)
        elif protocol_string.lower() == 'file':
            return URLDownloadTask(source_prefix)
        elif protocol_string.lower() == 'ftp':
            return URLDownloadTask(source_prefix)
        elif protocol_string.lower() == 'esri':
            return EsriRestDownloadTask(source_prefix)
        else:
            raise KeyError("I don't know how to extract for protocol {}".format(protocol_string))

    def download(self, source_urls, workdir, source_config):
        raise NotImplementedError()

def guess_url_file_extension(url):
    ''' Get a filename extension for a URL using various hints.
    '''
    scheme, _, path, _, query, _ = urlparse(url)
    mimetypes.add_type('application/x-zip-compressed', '.zip', False)
    mimetypes.add_type('application/vnd.geo+json', '.json', False)

    _, likely_ext = os.path.splitext(path)
    bad_extensions = '', '.cgi', '.php', '.aspx', '.asp', '.do'

    if not query and likely_ext not in bad_extensions:
        #
        # Trust simple URLs without meaningless filename extensions.
        #
        _L.debug(u'URL says "{}" for {}'.format(likely_ext, url))
        path_ext = likely_ext

    else:
        #
        # Get a dictionary of headers and a few bytes of content from the URL.
        #
        if scheme in ('http', 'https'):
            response = request('GET', url, stream=True)
            handle, file = mkstemp()

            for chunk in response.iter_content(chunk_size=8192):
                os.write(handle, chunk)

            os.close(handle)

            content_path = file
            headers = response.headers
            response.close()

        elif scheme in ('file', ''):
            headers = dict()
            content_path = path
        else:
            raise ValueError('Unknown scheme "{}": {}'.format(scheme, url))

        path_ext = False

        # Guess path extension from Content-Type header
        if 'content-type' in headers:
            content_type = headers['content-type'].split(';')[0]
            _L.debug('Content-Type says "{}" for {}'.format(content_type, url))
            path_ext = mimetypes.guess_extension(content_type, False)

            #
            # Uh-oh, see if Content-Disposition disagrees with Content-Type.
            # Socrata recently started using Content-Disposition instead
            # of normal response headers so it's no longer easy to identify
            # file type.
            #
            if 'content-disposition' in headers:
                pattern = r'attachment; filename=("?)(?P<filename>[^;]+)\1'
                match = re.match(pattern, headers['content-disposition'], re.I)
                if match:
                    _, attachment_ext = splitext(match.group('filename'))
                    if path_ext == attachment_ext:
                        _L.debug('Content-Disposition agrees: "{}"'.format(match.group('filename')))
                    else:
                        _L.debug('Content-Disposition disagrees: "{}" says we should use "{}", using "{}" instead'.format(
                            match.group('filename'),
                            attachment_ext,
                            path_ext,
                        ))

        if not path_ext:
            #
            # Headers didn't clearly define a known extension.
            # Instead, shell out to `file` to peek at the content.
            #
            mime_type = get_content_mimetype(content_path)
            _L.debug('file says "{}" for {}'.format(mime_type, url))
            path_ext = mimetypes.guess_extension(mime_type, False)

    return path_ext

def get_content_mimetype(path):
    ''' Get a mime-type for a short length of file content.
    '''
    mime_type = check_output(('file', '--mime-type', '-b', path)).strip()
    os.remove(path)

    return mime_type.decode('utf-8')

class URLDownloadTask(DownloadTask):
    CHUNK = 16 * 1024

    def get_file_path(self, url, dir_path):
        ''' Return a local file path in a directory for a URL.

            May need to fill in a filename extension based on HTTP Content-Type.
        '''
        scheme, host, path, _, _, _ = urlparse(url)
        path_base, _ = os.path.splitext(path)

        if self.source_prefix is None:
            # With no source prefix like "us-ca-oakland" use the name as given.
            name_base = os.path.basename(path_base)
        else:
            # With a source prefix, create a safe and unique filename with a hash.
            hash = sha1((host + path_base).encode('utf-8'))
            name_base = u'{}-{}'.format(self.source_prefix, hash.hexdigest()[:8])

        path_ext = guess_url_file_extension(url)
        _L.debug(u'Guessed {}{} for {}'.format(name_base, path_ext, url))

        return os.path.join(dir_path, name_base + path_ext)

    def download(self, source_urls, workdir, source_config):
        output_files = []
        download_path = os.path.join(workdir, 'http')
        mkdirsp(download_path)

        for source_url in source_urls:
            file_path = self.get_file_path(source_url, download_path)

            # FIXME: For URLs with file:// scheme, simply copy the file
            # to the expected location so that os.path.exists() returns True.
            # Instead, implement a FileDownloadTask class?
            scheme, _, path, _, _, _ = urlparse(source_url)
            if scheme == 'file':
                shutil.copy(path, file_path)

            if os.path.exists(file_path):
                output_files.append(file_path)
                _L.debug("File exists %s", file_path)
                continue

            try:
                resp = request('GET', source_url, headers=self.headers, stream=True)
            except Exception as e:
                raise DownloadError("Could not connect to URL", e)

            if resp.status_code in range(400, 499):
                raise DownloadError('{} response from {}'.format(resp.status_code, source_url))

            size = 0
            with open(file_path, 'wb') as fp:
                for chunk in resp.iter_content(self.CHUNK):
                    size += len(chunk)
                    fp.write(chunk)

            output_files.append(file_path)

            _L.info("Downloaded %s bytes for file %s", size, file_path)

        return output_files


class EsriRestDownloadTask(DownloadTask):

    def get_file_path(self, url, dir_path):
        ''' Return a local file path in a directory for a URL.
        '''
        _, host, path, _, _, _ = urlparse(url)
        hash, path_ext = sha1((host + path).encode('utf-8')), '.csv'

        # With no source prefix like "us-ca-oakland" use the host as a hint.
        name_base = '{}-{}'.format(self.source_prefix or host, hash.hexdigest()[:8])

        _L.debug('Downloading {} to {}{}'.format(url, name_base, path_ext))

        return os.path.join(dir_path, name_base + path_ext)

    @classmethod
    def fields_from_conform_function(cls, v):
        fxn = v.get('function')
        if not fxn:
            return set()

        if fxn in ('join', 'format'):
            # Join and format functions are a list of fields
            return set(v['fields'])
        elif fxn == 'chain':
            # Chain function is a list of functions that we should recurse into for field names
            fields = set()
            user_vars = set([v['variable']])
            for func in v['functions']:
                if isinstance(func, dict) and 'function' in func:
                    fields |= cls.fields_from_conform_function(func) - user_vars
            return fields
        elif fxn == 'constant':
            # Constant function doesn't use any fields
            return set()
        else:
            return set([v.get('field')])

    @classmethod
    def field_names_to_request(cls, source_config):
        ''' Return list of fieldnames to request based on conform, or None.
        '''

        conform = source_config.data_source.get('conform')

        if not conform:
            return None

        fields = set()
        for k, v in conform.items():
            if k in source_config.SCHEMA:
                if isinstance(v, dict):
                    # It's a function of some sort?
                    if 'function' in v:
                        fields |= cls.fields_from_conform_function(v)
                elif isinstance(v, list):
                    # It's a list of field names
                    fields |= set(v)
                else:
                    fields.add(v)

        if fields:
            # Remove any blank or None values
            return list(filter(None, sorted(fields)))
        else:
            return None

    def download(self, source_urls, workdir, source_config):
        output_files = []
        download_path = os.path.join(workdir, 'esri')
        mkdirsp(download_path)

        query_fields = EsriRestDownloadTask.field_names_to_request(source_config)
        conform = source_config.data_source.get('conform') or {}

        for source_url in source_urls:
            size = 0
            file_path = self.get_file_path(source_url, download_path)

            if os.path.exists(file_path):
                output_files.append(file_path)
                _L.debug("File exists %s", file_path)
                continue

            downloader = EsriDumper(source_url, parent_logger=_L, timeout=300)

            metadata = downloader.get_metadata()

            if query_fields is None:
                field_names = [f['name'] for f in metadata['fields']]
            else:
                field_names = query_fields[:]

            if GEOM_FIELDNAME not in field_names:
                field_names.append(GEOM_FIELDNAME)

            # Get the count of rows in the layer
            try:
                row_count = downloader.get_feature_count()
                _L.info("Source has {} rows".format(row_count))
            except EsriDownloadError:
                _L.info("Source doesn't support count")

            with open(file_path, 'w', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=field_names)
                writer.writeheader()

                for feature in downloader:
                    try:
                        geom = feature.get('geometry') or {}
                        row = feature.get('properties') or {}

                        # If the feature doesn't have a geometry, see if the conform has lat and lon fields specified
                        # and try to build a point geometry from them
                        if not geom and conform.get('lat') and conform.get('lon'):
                            lat_field_name = conform['lat']
                            lon_field_name = conform['lon']

                            # Don't support functions to build the geometry yet
                            if not isinstance(lat_field_name, str) or not isinstance(lon_field_name, str):
                                raise TypeError("lat and lon don't support functions yet")

                            try:
                                geom = {
                                    'type': 'Point',
                                    'coordinates': [float(row.get(lon_field_name)), float(row.get(lat_field_name))]
                                }
                            except (TypeError, ValueError):
                                raise TypeError("Couldn't build geometry from lat and lon fields")

                        if not geom:
                            raise TypeError("No geometry parsed")
                        if any((isinstance(g, float) and math.isnan(g)) for g in traverse(geom)):
                            raise TypeError("Geometry has NaN coordinates")

                        shp = shape(geom)
                        row[GEOM_FIELDNAME] = shp.wkt

                        writer.writerow({fn: row.get(fn) for fn in field_names})
                        size += 1
                    except TypeError:
                        _L.debug("Skipping a geometry", exc_info=True)

            _L.info("Downloaded %s ESRI features for file %s", size, file_path)
            output_files.append(file_path)
        return output_files

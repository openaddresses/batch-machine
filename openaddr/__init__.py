from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr')

from tempfile import mkdtemp, mkstemp
from os.path import realpath, join, splitext, exists, dirname, abspath, relpath
from shutil import copy, move, rmtree
from os import close, utime, remove
from urllib.parse import urlparse
from datetime import datetime, date
import requests

from .cache import (
    CacheResult,
    compare_cache_details,
    DownloadTask,
    URLDownloadTask,
)

from .conform import (
    ConformResult,
    DecompressionTask,
    ExcerptDataTask,
    ConvertToCsvTask,
    elaborate_filenames,
    conform_license,
    conform_attribution,
    conform_sharealike,
    ADDRESSES_SCHEMA,
    BUILDINGS_SCHEMA,
    PARCELS_SCHEMA,
)

with open(join(dirname(__file__), 'VERSION')) as file:
    __version__ = file.read().strip()

class SourceConfig:
    def __init__(self, source, layer, layersource):
        self.source = source
        self.layer = layer
        self.layersource = layersource
        self.data_source = None
        self.data_source_name = self.layer + '-' + self.layersource

        for ds in source['layers'][layer]:
            if ds.get('name', None) == layersource:
                self.data_source = ds
                break

        if self.layer == 'addresses':
            self.SCHEMA = ADDRESSES_SCHEMA
        elif self.layer == "buildings":
            self.SCHEMA = BUILDINGS_SCHEMA
        elif self.layer == 'parcels':
            self.SCHEMA = PARCELS_SCHEMA

def cache(source_config, destdir, extras):
    ''' Python wrapper for openaddress-cache.

        Return a CacheResult object:

          cache: URL of cached data, possibly with file:// schema
          fingerprint: md5 hash of data,
          version: data version as date?
          elapsed: elapsed time as timedelta object
          output: subprocess output as string

        Creates and destroys a subdirectory in destdir.
    '''
    start = datetime.now()
    workdir = mkdtemp(prefix='cache-', dir=destdir)

    source_config.data_source.update(extras)

    source_urls = source_config.data_source.get('data')
    if not isinstance(source_urls, list):
        source_urls = [source_urls]

    protocol_string = source_config.data_source.get('protocol')

    task = DownloadTask.from_protocol_string(protocol_string, source_config)
    downloaded_files = task.download(source_urls, workdir, source_config)

    # FIXME: I wrote the download stuff to assume multiple files because
    # sometimes a Shapefile fileset is splayed across multiple files instead
    # of zipped up nicely. When the downloader downloads multiple files,
    # we should zip them together before uploading to S3 instead of picking
    # the first one only.
    filepath_to_upload = abspath(downloaded_files[0])

    #
    # Find the cached data and hold on to it.
    #
    resultdir = join(destdir, 'cached')
    source_config.data_source['cache'], source_config.data_source['fingerprint'] \
        = compare_cache_details(filepath_to_upload, resultdir, source_config.data_source)

    rmtree(workdir)

    return CacheResult(source_config.data_source.get('cache', None),
                       source_config.data_source.get('fingerprint', None),
                       source_config.data_source.get('version', None),
                       datetime.now() - start)

def conform(source_config, destdir, extras):
    ''' Python wrapper for openaddresses-conform.

        Return a ConformResult object:

          processed: URL of processed data CSV
          path: local path to CSV of processed data
          geometry_type: typically Point or Polygon
          elapsed: elapsed time as timedelta object
          output: subprocess output as string

        Creates and destroys a subdirectory in destdir.
    '''
    start = datetime.now()
    workdir = mkdtemp(prefix='conform-', dir=destdir)

    source_config.data_source.update(extras)

    #
    # The cached data will be a local path.
    #
    scheme, _, cache_path, _, _, _ = urlparse(extras.get('cache', ''))
    if scheme == 'file':
        copy(cache_path, workdir)

    source_urls = source_config.data_source.get('cache')
    if not isinstance(source_urls, list):
        source_urls = [source_urls]

    task1 = URLDownloadTask(source_config.data_source_name)
    downloaded_path = task1.download(source_urls, workdir, source_config)
    _L.info("Downloaded to %s", downloaded_path)

    task2 = DecompressionTask.from_format_string(source_config.data_source.get('compression'))
    names = elaborate_filenames(source_config.data_source.get('conform', {}).get('file', None))
    decompressed_paths = task2.decompress(downloaded_path, workdir, names)
    _L.info("Decompressed to %d files", len(decompressed_paths))

    task3 = ExcerptDataTask()
    try:
        conform = source_config.data_source.get('conform', {})
        data_sample, geometry_type = task3.excerpt(decompressed_paths, workdir, conform)
        _L.info("Sampled %d records", len(data_sample))
    except Exception as e:
        _L.warning("Error doing excerpt; skipping", exc_info=True)
        data_sample = None
        geometry_type = None

    task4 = ConvertToCsvTask()
    try:
        csv_path, addr_count = task4.convert(source_config, decompressed_paths, workdir)
        if addr_count > 0:
            _L.info("Converted to %s with %d addresses", csv_path, addr_count)
        else:
            _L.warning('Found no addresses in source data')
            csv_path = None
    except Exception as e:
        _L.warning("Error doing conform; skipping", exc_info=True)
        csv_path, addr_count = None, 0

    out_path = None
    if csv_path is not None and exists(csv_path):
        move(csv_path, join(destdir, 'out.csv'))
        out_path = realpath(join(destdir, 'out.csv'))

    rmtree(workdir)

    sharealike_flag = conform_sharealike(source_config.data_source.get('license'))
    attr_flag, attr_name = conform_attribution(source_config.data_source.get('license'), source_config.data_source.get('attribution'))

    return ConformResult(source_config.data_source.get('processed', None),
                         data_sample,
                         source_config.data_source.get('website'),
                         conform_license(source_config.data_source.get('license')),
                         geometry_type,
                         addr_count,
                         out_path,
                         datetime.now() - start,
                         sharealike_flag,
                         attr_flag,
                         attr_name)

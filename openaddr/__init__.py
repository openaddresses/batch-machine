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
    ConvertToGeojsonTask,
    elaborate_filenames,
    ADDRESSES_SCHEMA,
    NAD_SCHEMA,
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
        elif self.layer == 'NAD':
            self.SCHEMA = NAD_SCHEMA
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

    compression = source_config.data_source.get('compression')
    if compression:
        task2 = DecompressionTask.from_format_string(compression)
        names = elaborate_filenames(source_config.data_source.get('conform', {}).get('file', None))
        decompressed_paths = task2.decompress(downloaded_path, workdir, names)
        _L.info("Decompressed to %d files", len(decompressed_paths))
    else:
        decompressed_paths = downloaded_path
        _L.info("No decompression requested")

    task4 = ConvertToGeojsonTask()
    try:
        out_path, feat_count = task4.convert(source_config, decompressed_paths, workdir)
        if feat_count > 0:
            _L.info("Converted to %s with %d features", out_path, feat_count)
        else:
            _L.warning('Found no features in source data')
            out_path = None
    except Exception as e:
        _L.warning("Error doing conform; skipping", exc_info=True)
        out_path, feat_count = None, 0

    if out_path is not None and exists(out_path):
        move(out_path, join(destdir, 'out.geojson'))
        out_path = realpath(join(destdir, 'out.geojson'))

    rmtree(workdir)

    return ConformResult(source_config.data_source.get('processed', None),
                         feat_count,
                         out_path,
                         datetime.now() - start)

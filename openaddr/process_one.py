from __future__ import absolute_import, division, print_function
import logging

_L = logging.getLogger('openaddr.process_one')

from urllib.parse import urlparse
from os.path import join, basename, dirname, exists, splitext, relpath
from shutil import copy, move, rmtree
from argparse import ArgumentParser
from os import mkdir, rmdir, close, chmod
from shapely.wkt import loads as wkt_loads
from shapely.geometry import mapping
from _thread import get_ident
import tempfile, json, csv, sys, enum
import threading

from . import util, cache, conform, preview, slippymap, CacheResult, ConformResult, __version__, SourceConfig
from .cache import DownloadError
from .conform import check_source_tests

from esridump.errors import EsriDownloadError

class SourceSaysSkip(RuntimeError): pass
class SourceTestsFailed(RuntimeError): pass

@enum.unique
class SourceProblem (enum.Enum):
    ''' Possible problems encountered in a source.
    '''
    skip_source = 'Source says to skip'
    missing_conform = 'Source is missing a conform object'
    unknown_conform_format = 'Unknown source conform format'
    unknown_conform_protocol = 'Unknown source conform protocol'
    download_source_failed = 'Could not download source data'
    conform_source_failed = 'Could not conform source data'
    no_coverage = 'Missing or incomplete coverage'
    no_esri_token = 'Missing required ESRI token'
    test_failed = 'An acceptance test failed'
    no_features_found = 'Found no features in source data'

    # Old tag naming; replaced with "format" or "protocol"
    unknown_conform_type = 'Unknown source conform type'

def boolstr(value):
    '''
    '''
    if value is True:
        return 'true'

    if value is False:
        return 'false'

    if value is None:
        return ''

    raise ValueError(repr(value))

def process(source, destination, layer, layersource,
            do_preview, do_pmtiles,
            protomaps_key=None, extras=dict()):
    ''' Process a single source and destination, return path to JSON state file.

        Creates a new directory and files under destination.
    '''
    # The main processing thread holds wait_lock until it is done.
    # The logging thread periodically writes data in the background,
    # then exits once the main thread releases the lock.
    wait_lock = threading.Lock()
    proc_wait = threading.Thread(target=util.log_process_usage, args=(wait_lock, ))

    temp_dir = tempfile.mkdtemp(prefix='process_one-', dir=destination)
    temp_src = join(temp_dir, basename(source))
    copy(source, temp_src)

    state_path = False
    data_source = dict(name='')

    log_handler = get_log_handler(temp_dir)
    logging.getLogger('openaddr').addHandler(log_handler)

    with wait_lock:
        proc_wait.start()
        cache_result, conform_result = CacheResult.empty(), ConformResult.empty()
        preview_path, pmtiles_path, skipped_source = None, None, False
        tests_passed = None

        try:
            with open(temp_src) as file:
                source = json.load(file)

                # Update a v1 source to v2 and set required flags to process it
                if source.get('schema', None) == None and source.get('layers', None) == None:
                    source = upgrade_source_schema(source)
                    layer = 'addresses'
                    layersource = 'primary'

                if type(layer) is not str:
                    layer = ''
                if type(layersource) is not str:
                    layersource = ''

                if len(layer) == 0:
                    _L.error('explicit --layer arg is required for v2 sources')
                    raise ValueError('explicit --layer arg is required for v2 sources')
                elif len(layersource) == 0:
                    _L.error('explicit --layersource arg is required for v2 sources')
                    raise ValueError('explicit --layersource arg is required for v2 sources')

                # Only Address Layers are supported right now
                if (layer != 'addresses' and layer != 'parcels' and layer != 'buildings'):
                    _L.error('Nothing processed: \'{}\' layer not currently supported'.format(layer))
                    raise ValueError('Nothing processed: \'{}\' layer not currently supported')
                elif source['layers'].get(layer, None) == None:
                    _L.error('Nothing processed: \'{}\' layer does not exist in source'.format(layer))
                    raise ValueError('Nothing processed: \'{}\' layer does not exist in source')

                source_config = SourceConfig(source, layer, layersource)

                if source_config.data_source == False or source_config.data_source == None:
                    _L.error('Nothing processed: \'{}\' layersource not found in \'{}\' layer '.format(layersource, layer))
                    raise ValueError('Nothing processed: \'{}\' layersource not found in \'{}\' layer')

                if source_config.data_source.get('skip', None):
                    raise SourceSaysSkip()

                # Check tests in data_source object.
                tests_passed, failure_details = check_source_tests(source_config)
                if tests_passed is False:
                    raise SourceTestsFailed(failure_details)

                if source_config.data_source.get('name', None) == None:
                    _L.warning('name attribute is required on each data source')
                    raise ValueError('name attribute is required on each data source')

                # Cache source data.
                try:
                    cache_result = cache(source_config, temp_dir, extras)
                except EsriDownloadError as e:
                    _L.warning('Could not download ESRI source data: {}'.format(e))
                    raise
                except DownloadError as e:
                    _L.warning('Could not download source data')
                    raise

                if not cache_result.cache:
                    _L.warning('Nothing cached')
                else:
                    _L.info(u'Cached data in {}'.format(cache_result.cache))

                    # Conform cached source data.
                    conform_result = conform(source_config, temp_dir, cache_result.todict())

                    if not conform_result.path:
                        _L.warning('Nothing processed')
                    else:
                        _L.info('Processed data in {}'.format(conform_result.path))

                        if do_preview and protomaps_key:
                            preview_path = render_preview(conform_result.path, temp_dir, protomaps_key)

                        if not preview_path:
                            _L.warning('Nothing previewed')
                        else:
                            _L.info('Preview image in {}'.format(preview_path))

                        if do_pmtiles:
                            pmtiles_path = render_pmtiles(conform_result.path, temp_dir)

                            if not pmtiles_path:
                                _L.warning('No pmtiles generated')
                            else:
                                _L.info('pmtiles file in {}'.format(pmtiles_path))

        except SourceSaysSkip:
            _L.info('Source says to skip in process_one.process()')
            skipped_source = True

        except SourceTestsFailed as e:
            _L.warning('A source test failed in process_one.process(): %s', str(e))
            tests_passed = False

        except Exception:
            _L.warning('Error in process_one.process()', exc_info=True)

        finally:
            # Make sure this gets done no matter what
            logging.getLogger('openaddr').removeHandler(log_handler)

        state_path = write_state(temp_src, layer, data_source['name'], skipped_source, destination, log_handler,
            tests_passed, cache_result, conform_result, preview_path, pmtiles_path, temp_dir)

        log_handler.close()
        rmtree(temp_dir)

    # TODO Return List of state paths
    return state_path

def upgrade_source_schema(schema):
    ''' Temporary Shim to convert a V1 Schema source (layerless) to a V2 schema file (layers)
    '''

    v2 = { 'layers': { 'addresses': [{ 'name': 'primary' }] } }

    for k, v in schema.items():
        if (k == 'coverage'):
            v2['coverage'] = v
        else:
            v2['layers']['addresses'][0][k] = v

    return v2

def render_preview(csv_filename, temp_dir, protomaps_key):
    '''
    '''
    png_filename = join(temp_dir, 'preview.png')
    preview.render(csv_filename, png_filename, 668, 2, protomaps_key)

    return png_filename


def render_pmtiles(csv_filename, temp_dir):
    '''
    :param csv_filename:
    :param temp_dir:
    :return:
    '''
    try:
        pmtiles_filename = join(temp_dir, 'slippymap.pmtiles')
        slippymap.generate(pmtiles_filename, csv_filename)
    except Exception as e:
        _L.error('%s in render_pmtiles: %s', type(e), e)
        return None
    else:
        return pmtiles_filename

class LogFilterCurrentThread:
    ''' Logging filter object to match only record in the current thread.
    '''
    def __init__(self):
        # Seems to work as unique ID with multiprocessing.Process() as well as threading.Thread()
        self.thread_id = get_ident()

    def filter(self, record):
        return record.thread == self.thread_id

def get_log_handler(directory):
    ''' Create a new file handler and return it.
    '''
    handle, filename = tempfile.mkstemp(dir=directory, suffix='.log')
    close(handle)
    chmod(filename, 0o644)

    handler = logging.FileHandler(filename)
    handler.setFormatter(logging.Formatter(u'%(asctime)s %(levelname)08s: %(message)s'))
    handler.setLevel(logging.DEBUG)

    # # Limit log messages to the current thread
    # handler.addFilter(LogFilterCurrentThread())

    return handler

def find_source_problem(log_contents, source):
    print(log_contents)
    '''
    '''
    if 'WARNING: A source test failed' in log_contents:
        return SourceProblem.test_failed

    if 'WARNING: Source is missing a conform object' in log_contents:
        return SourceProblem.missing_conform

    if 'WARNING: Unknown source conform protocol' in log_contents:
        return SourceProblem.unknown_conform_protocol

    if 'WARNING: Unknown source conform format' in log_contents:
        return SourceProblem.unknown_conform_format

    if 'WARNING: Unknown source conform type' in log_contents:
        return SourceProblem.unknown_conform_type

    if 'WARNING: Found no features in source data' in log_contents:
        return SourceProblem.no_features_found

    if 'WARNING: Could not download source data' in log_contents:
        return SourceProblem.download_source_failed

    if 'WARNING: Error doing conform; skipping' in log_contents:
        return SourceProblem.conform_source_failed

    if 'WARNING: Could not download ESRI source data: Could not retrieve layer metadata: Token Required' in log_contents:
        return SourceProblem.no_esri_token

    if 'coverage' in source:
        coverage = source.get('coverage')
        if 'US Census' in coverage or 'ISO 3166' in coverage or 'geometry' in coverage:
            pass
        else:
            return SourceProblem.no_coverage
    else:
        return SourceProblem.no_coverage

    return None

def write_state(source, layer, data_source_name, skipped, destination, log_handler, tests_passed,
                cache_result, conform_result, preview_path, pmtiles_path,
                temp_dir):
    '''
    '''
    source_id, _ = splitext(basename(source))
    statedir = join(destination, source_id)

    if not exists(statedir):
        mkdir(statedir)

    statedir = join(statedir, layer)
    if not exists(statedir):
        mkdir(statedir)

    statedir = join(statedir, data_source_name)
    if not exists(statedir):
        mkdir(statedir)

    if cache_result.cache:
        scheme, _, cache_path1, _, _, _ = urlparse(cache_result.cache)
        if scheme in ('file', ''):
            cache_path2 = join(statedir, 'cache{1}'.format(*splitext(cache_path1)))
            copy(cache_path1, cache_path2)
            state_cache = relpath(cache_path2, statedir)
        else:
            state_cache = cache_result.cache
    else:
        state_cache = None

    if conform_result.path:
        _, _, processed_path1, _, _, _ = urlparse(conform_result.path)
        processed_path2 = join(statedir, 'out{1}'.format(*splitext(processed_path1)))
        copy(processed_path1, processed_path2)

    # Write the sample data to a sample.json file
    if conform_result.sample:
        sample_path = join(statedir, 'sample.json')
        with open(sample_path, 'w') as sample_file:
            json.dump(conform_result.sample, sample_file, indent=2)

    if preview_path:
        preview_path2 = join(statedir, 'preview.png')
        copy(preview_path, preview_path2)

    if pmtiles_path:
        pmtiles_path2 = join(statedir, 'slippymap.pmtiles')
        copy(pmtiles_path, pmtiles_path2)

    log_handler.flush()
    output_path = join(statedir, 'output.txt')
    copy(log_handler.stream.name, output_path)

    if skipped:
        source_problem = SourceProblem.skip_source
    else:
        with open(output_path) as file:
            log_content = file.read()
        if exists(source):
            with open(source) as file:
                source_data = json.load(file)
        else:
            source_data = {}

        source_problem = find_source_problem(log_content, source_data)

    state = [
        ('source', basename(source)),
        ('skipped', bool(skipped)),
        ('cache', state_cache),
        ('sample', conform_result.sample and relpath(sample_path, statedir)),
        ('feat count', conform_result.feat_count),
        ('version', cache_result.version),
        ('fingerprint', cache_result.fingerprint),
        ('cache time', cache_result.elapsed and str(cache_result.elapsed)),
        ('processed', conform_result.path and relpath(processed_path2, statedir)),
        ('process time', conform_result.elapsed and str(conform_result.elapsed)),
        ('output', relpath(output_path, statedir)),
        ('preview', preview_path and relpath(preview_path2, statedir)),
        ('pmtiles', pmtiles_path and relpath(pmtiles_path2, statedir)),
        ('source problem', getattr(source_problem, 'value', None)),
        ('code version', __version__),
        ('tests passed', tests_passed),
        ]

    with open(join(statedir, 'index.txt'), 'w', encoding='utf8') as file:
        out = csv.writer(file, dialect='excel-tab')
        for row in zip(*state):
            out.writerow(row)

    with open(join(statedir, 'index.json'), 'w') as file:
        json.dump(list(zip(*state)), file, indent=2)

        _L.info(u'Wrote to state: {}'.format(file.name))
        return file.name

parser = ArgumentParser(description='Run one source file locally, prints output path.')

parser.add_argument('source', help='Required source file name.')
parser.add_argument('destination', help='Required output directory name.')

parser.add_argument('-ln', '--layer', help='Layer name to process in V2 sources',
                    dest='layer', default='')
parser.add_argument('-ls', '--layersource', help='Source within a given layer to pull from',
                    dest='layersource', default='')

parser.add_argument('--skip-preview', help="Don't render a map preview",
                    action='store_const', dest='render_preview',
                    const=False, default=True)

parser.add_argument('--render-preview', help="Render a map preview",
                    action='store_const', dest='render_preview',
                    const=True, default=True)

parser.add_argument('--protomaps-key', dest='protomaps_key',
                    help='Protomaps API Key. See: https://protomaps.com/dashboard')

parser.add_argument('-l', '--logfile', help='Optional log file name.')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

def main():
    '''
    '''

    # Get a handle for the openaddr logger and its children
    openaddr_logger = logging.getLogger('openaddr')
    logger_state = {'previous loglevel': openaddr_logger.level}

    # Default logging format.
    log_format = '%(asctime)s %(levelname)07s: %(message)s'

    # Set the logger level to show everything, and filter down in the handlers.
    openaddr_logger.setLevel(logging.DEBUG)

    # Set up a logger to stderr
    handler1 = logging.StreamHandler()
    handler1.setLevel(logging.DEBUG)
    handler1.setFormatter(logging.Formatter(log_format))
    openaddr_logger.addHandler(handler1)
    logger_state['stream handler'] = handler1

    args = parser.parse_args()

    # Can't generate preview without Protomaps key
    if args.render_preview and not args.protomaps_key:
        _L.error('Protomaps key is required to generate preview')
        return 1

    # Allow CSV files with very long fields
    csv.field_size_limit(sys.maxsize)

    try:
        processed_path = process(args.source, args.destination,
                                 args.layer, args.layersource,
                                 args.render_preview,
                                 args.render_preview,
                                 protomaps_key=args.protomaps_key)
    except Exception as e:
        _L.error(e, exc_info=True)
        return 1
    else:
        print(processed_path)
        return 0

if __name__ == '__main__':
    exit(main())

# coding=ascii

from __future__ import absolute_import, division, print_function
import logging; _L = logging.getLogger('openaddr.conform')

import os
import errno
import tempfile
import mimetypes
import json
import copy
import csv
import re
import osgeo

from .geojson import stream_geojson

from zipfile import ZipFile
from locale import getpreferredencoding
from os.path import splitext
from hashlib import sha1
from uuid import uuid4

from osgeo import ogr, osr, gdal
ogr.UseExceptions()

def gdal_error_handler(err_class, err_num, err_msg):
    errtype = {
            gdal.CE_None:'None',
            gdal.CE_Debug:'Debug',
            gdal.CE_Warning:'Warning',
            gdal.CE_Failure:'Failure',
            gdal.CE_Fatal:'Fatal'
    }
    err_msg = err_msg.replace('\n',' ')
    err_class = errtype.get(err_class, 'None')
    _L.error("GDAL gave %s %s: %s", err_class, err_num, err_msg)
gdal.PushErrorHandler(gdal_error_handler)

# Field names for use in cached CSV files.
# We add columns to the extracted CSV with our own data with these names.
GEOM_FIELDNAME = 'OA:GEOM'

ADDRESSES_SCHEMA = [ 'NUMBER', 'STREET', 'UNIT', 'CITY', 'DISTRICT', 'REGION', 'POSTCODE', 'ID' ]
BUILDINGS_SCHEMA = []
PARCELS_SCHEMA = [ 'PID', 'OWNERS' ]
RESERVED_SCHEMA = ADDRESSES_SCHEMA + BUILDINGS_SCHEMA + PARCELS_SCHEMA + [
    "LAT",
    "LON"
]

UNZIPPED_DIRNAME = 'unzipped'

# extracts:
# - '123' from '123 Main St'
# - '123 1/2' from '123 1/2 Main St'
# - '123-1/2' from '123-1/2 Main St'
# - '123-1' from '123-1 Main St'
# - '123a' from '123a Main St'
# - '123-a' from '123-a Main St'
# - '' from '3rd St' (the 3 belongs to the street, it's not a house number)
#
# this regex can be optimized but number scenarios are much cleaner this way:
# - just digits with optional fractional
# - two groups of digits separated by a hyphen (for queens-style addresses, eg - 69-15 51st Ave)
# - digits and a letter, optionally separated by a hyphen
prefixed_number_pattern = re.compile("^\s*(\d+(?:[ -]\d/\d)?|\d+-\d+|\d+-?[A-Z])\s+", re.IGNORECASE)

# extracts:
# - 'Main St' from '123 Main St'
# - 'Main St' from '123 1/2 Main St'
# - 'Main St' from '123-1/2 Main St'
# - 'Main St' from '123-1 Main St'
# - 'Main St' from '123a Main St'
# - 'Main St' from '123-a Main St'
# - 'Main St' from 'Main St'
#
# like prefixed_number_pattern, this regex can be optimized but this is cleaner
postfixed_street_pattern = re.compile("^(?:\s*(?:\d+(?:[ -]\d/\d)?|\d+-\d+|\d+-?[A-Z])\s+)?(.*)", re.IGNORECASE)

# extracts:
# - 'Main Street' from '123 Main Street Unit 3'
# - 'Main Street' from '123 Main Street Apartment 3'
# - 'Main Street' from '123 Main Street Apt 3'
# - 'Main Street' from '123 Main Street Apt. 3'
# - 'Main Street' from '123 Main Street Suite 3'
# - 'Main Street' from '123 Main Street Ste 3'
# - 'Main Street' from '123 Main Street Ste. 3'
# - 'Main Street' from '123 Main Street Building 3'
# - 'Main Street' from '123 Main Street Bldg 3'
# - 'Main Street' from '123 Main Street Bldg. 3'
# - 'Main Street' from '123 Main Street Lot 3'
# - 'Main Street' from '123 Main Street #3'
# - 'Main Street' from '123 Main Street # 3'
# This regex contains 3 groups: optional house number, street, optional unit
# only street is a matching group, house number and unit are non-matching
postfixed_street_with_units_pattern = re.compile("^(?:\s*(?:\d+(?:[ -]\d/\d)?|\d+-\d+|\d+-?[A-Z])\s+)?(.+?)(?:\s+(?:(?:UNIT|APARTMENT|APT\.?|SUITE|STE\.?|BUILDING|BLDG\.?|LOT)\s+|#).+)?$", re.IGNORECASE)

# extracts:
# - 'Unit 3' from 'Main Street Unit 3'
# - 'Apartment 3' from 'Main Street Apartment 3'
# - 'Apt 3' from 'Main Street Apt 3'
# - 'Apt. 3' from 'Main Street Apt. 3'
# - 'Suite 3' from 'Main Street Suite 3'
# - 'Ste 3' from 'Main Street Ste 3'
# - 'Ste. 3' from 'Main Street Ste. 3'
# - 'Building 3' from 'Main Street Building 3'
# - 'Bldg 3' from 'Main Street Bldg 3'
# - 'Bldg. 3' from 'Main Street Bldg. 3'
# - 'Lot 3' from 'Main Street Lot 3'
# - '#3' from 'Main Street #3'
# - '# 3' from 'Main Street # 3'
postfixed_unit_pattern = re.compile("\s((?:(?:UNIT|APARTMENT|APT\.?|SUITE|STE\.?|BUILDING|BLDG\.?|LOT)\s+|#).+)$", re.IGNORECASE)

def mkdirsp(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


class ConformResult:
    processed = None
    sample = None
    feat_count = None
    path = None
    elapsed = None

    def __init__(self, processed, feat_count, path, elapsed):
        self.processed = processed
        self.feat_count = feat_count
        self.path = path
        self.elapsed = elapsed

    @staticmethod
    def empty():
        return ConformResult(None, None, None, None)

    def todict(self):
        return dict(processed=self.processed, sample=self.sample)


class DecompressionError(Exception):
    pass

class DecompressionTask(object):
    @classmethod
    def from_format_string(clz, format_string):
        if format_string == None:
            return GuessDecompressTask()
        elif format_string.lower() == 'zip':
            return ZipDecompressTask()
        else:
            raise KeyError("I don't know how to decompress for format {}".format(format_string))

    def decompress(self, source_paths):
        raise NotImplementedError()


class GuessDecompressTask(DecompressionTask):
    ''' Decompression task that tries to guess compression from file names.
    '''
    def decompress(self, source_paths, workdir, filenames):
        types = {type for (type, _) in map(mimetypes.guess_type, source_paths)}

        if types == {'application/zip'}:
            substitute_task = ZipDecompressTask()
            _L.info('Guessing zip compression based on file names')
            return substitute_task.decompress(source_paths, workdir, filenames)

        _L.warning('Could not guess a single compression from file names')
        return source_paths

def is_in(path, names):
    '''
    '''
    if path.lower() in names:
        # Found it!
        return True

    for name in names:
        # Maybe one of the names is an enclosing directory?
        if not os.path.relpath(path.lower(), name).startswith('..'):
            # Yes, that's it.
            return True

    return False

class ZipDecompressTask(DecompressionTask):
    def decompress(self, source_paths, workdir, filenames):
        output_files = []
        expand_path = os.path.join(workdir, UNZIPPED_DIRNAME)
        mkdirsp(expand_path)

        # Extract contents of zip file into expand_path directory.
        for source_path in source_paths:
            with ZipFile(source_path, 'r') as z:
                for name in z.namelist():
                    if len(filenames) and not is_in(name, filenames):
                        # Download only the named file, if any.
                        _L.debug("Skipped file {}".format(name))
                        continue

                    z.extract(name, expand_path)

        # Collect names of directories and files in expand_path directory.
        for (dirpath, dirnames, filenames) in os.walk(expand_path):
            for dirname in dirnames:
                if os.path.splitext(dirname)[-1].lower() == '.gdb':
                    output_files.append(os.path.join(dirpath, dirname))
                    _L.debug("Expanded directory {}".format(output_files[-1]))
            for filename in filenames:
                output_files.append(os.path.join(dirpath, filename))
                _L.debug("Expanded file {}".format(output_files[-1]))

        return output_files

def elaborate_filenames(filename):
    ''' Return a list of filenames for a single name from conform file tag.

        Used to expand example.shp with example.shx, example.dbf, and example.prj.
    '''
    if filename is None:
        return []

    filename = filename.lower()
    base, original_ext = splitext(filename)

    if original_ext == '.shp':
        return [base + ext for ext in (original_ext, '.shx', '.dbf', '.prj')]

    return [filename]

def guess_source_encoding(datasource, layer):
    ''' Guess at a string encoding using hints from OGR and locale().

        Duplicate the process used in Fiona, described and implemented here:

        https://github.com/openaddresses/machine/issues/42#issuecomment-69693143
        https://github.com/Toblerity/Fiona/blob/53df35dc70fb/docs/encoding.txt
        https://github.com/Toblerity/Fiona/blob/53df35dc70fb/fiona/ogrext.pyx#L386
    '''
    ogr_recoding = layer.TestCapability(ogr.OLCStringsAsUTF8)
    is_shapefile = datasource.GetDriver().GetName() == 'ESRI Shapefile'

    return (ogr_recoding and 'UTF-8') \
        or (is_shapefile and 'ISO-8859-1') \
        or getpreferredencoding()

def find_source_path(data_source, source_paths):
    ''' Figure out which of the possible paths is the actual source
    '''
    try:
        conform = data_source["conform"]
    except KeyError:
        _L.warning('Source is missing a conform object')
        raise

    format_string = conform.get('format')
    protocol_string = data_source.get('protocol')

    if format_string in ("shapefile"):
        # TODO this code is too complicated; see XML variant below for simpler option
        # Shapefiles are named *.shp
        candidates = []
        for fn in source_paths:
            basename, ext = os.path.splitext(fn)
            if ext.lower() == ".shp":
                candidates.append(fn)
        if len(candidates) == 0:
            _L.warning("No shapefiles found in %s", source_paths)
            return None
        elif len(candidates) == 1:
            _L.debug("Selected %s for source", candidates[0])
            return candidates[0]
        else:
            # Multiple candidates; look for the one named by the file attribute
            if "file" not in conform:
                _L.warning("Multiple shapefiles found, but source has no file attribute.")
                return None
            source_file_name = conform["file"]
            for c in candidates:
                if source_file_name == os.path.basename(c):
                    return c
            _L.warning("Source names file %s but could not find it", source_file_name)
            return None
    elif format_string == "geojson" and protocol_string != "ESRI":
        candidates = []
        for fn in source_paths:
            basename, ext = os.path.splitext(fn)
            if ext.lower() in (".json", ".geojson"):
                candidates.append(fn)
        if len(candidates) == 0:
            _L.warning("No JSON found in %s", source_paths)
            return None
        elif len(candidates) == 1:
            _L.debug("Selected %s for source", candidates[0])
            return candidates[0]
        else:
            _L.warning("Found more than one JSON file in source, can't pick one")
            # geojson spec currently doesn't include a file attribute. Maybe it should?
            return None
    elif format_string == "geojson" and protocol_string == "ESRI":
        # Old style ESRI conform: ESRI downloader should only give us a single cache.csv file
        return source_paths[0]
    elif format_string == "csv":
        # Return file if it's specified, else return the first file we find
        if "file" in conform:
            for fn in source_paths:
                # Consider it a match if the basename matches; directory names are a mess
                if os.path.basename(conform["file"]) == os.path.basename(fn):
                    return fn
            _L.warning("Conform named %s as file but we could not find it." % conform["file"])
            return None
        # See if a file has a CSV extension
        for fn in source_paths:
            if os.path.splitext(fn)[1].lower() == '.csv':
                return fn
        # Nothing else worked so just return the first one.
        return source_paths[0]
    elif format_string == "gdb":
        candidates = []
        for fn in source_paths:
            fn = re.sub('\.gdb.*', '.gdb', fn)
            basename, ext = os.path.splitext(fn)
            if ext.lower() == ".gdb" and fn not in candidates:
                candidates.append(fn)
        if len(candidates) == 0:
            _L.warning("No GDB found in %s", source_paths)
            return None
        elif len(candidates) == 1:
            _L.debug("Selected %s for source", candidates[0])
            return candidates[0]
        else:
            # Multiple candidates; look for the one named by the file attribute
            if "file" not in conform:
                _L.warning("Multiple GDBs found, but source has no file attribute.")
                return None
            source_file_name = conform["file"]
            for c in candidates:
                if source_file_name == os.path.basename(c):
                    return c
            _L.warning("Source names file %s but could not find it", source_file_name)
            return None
    elif format_string == "xml":
        # Return file if it's specified, else return the first .gml file we find
        if "file" in conform:
            for fn in source_paths:
                # Consider it a match if the basename matches; directory names are a mess
                if os.path.basename(conform["file"]) == os.path.basename(fn):
                    return fn
            _L.warning("Conform named %s as file but we could not find it." % conform["file"])
            return None
        else:
            for fn in source_paths:
                _, ext = os.path.splitext(fn)
                if ext == ".gml":
                    return fn
            _L.warning("Could not find a .gml file")
            return None
    else:
        _L.warning("Unknown source conform format %s", format_string)
        return None

class ConvertToCsvTask(object):
    known_types = ('.shp', '.json', '.csv', '.kml', '.gdb')

    def convert(self, source_config, source_paths, workdir):
        "Convert a list of source_paths and write results in workdir"
        _L.debug("Converting to %s", workdir)

        # Create a subdirectory "converted" to hold results
        output_file = None
        convert_path = os.path.join(workdir, 'converted')
        mkdirsp(convert_path)

        # Find the source and convert it
        source_path = find_source_path(source_config.data_source, source_paths)
        if source_path is not None:
            basename, ext = os.path.splitext(os.path.basename(source_path))
            dest_path = os.path.join(convert_path, basename + ".csv")
            rc = conform_cli(source_config, source_path, dest_path)
            if rc == 0:
                with open(dest_path) as file:
                    addr_count = sum(1 for line in file) - 1

                # Success! Return the path of the output CSV
                return dest_path, addr_count

        # Conversion must have failed
        return None, 0

def convert_regexp_replace(replace):
    ''' Convert regular expression replace string from $ syntax to slash-syntax.

        Replace one kind of replacement, then call self recursively to find others.
    '''
    if re.search(r'\$\d+\b', replace):
        # $dd* back-reference followed by a word break.
        return convert_regexp_replace(re.sub(r'\$(\d+)\b', r'\\\g<1>', replace))

    if re.search(r'\$\d+\D', replace):
        # $dd* back-reference followed by an non-digit character.
        return convert_regexp_replace(re.sub(r'\$(\d+)(\D)', r'\\\g<1>\g<2>', replace))

    if re.search(r'\$\{\d+\}', replace):
        # ${dd*} back-reference.
        return convert_regexp_replace(re.sub(r'\$\{(\d+)\}', r'\\g<\g<1>>', replace))

    return replace

def normalize_ogr_filename_case(source_path):
    '''
    '''
    base, ext = splitext(source_path)

    if ext == ext.lower():
        # Extension is already lowercase, no need to do anything.
        return source_path

    normal_path = base + ext.lower()

    if os.path.exists(normal_path):
        # We appear to be on a case-insensitive filesystem.
        return normal_path

    os.link(source_path, normal_path)

    # May need to deal with some additional files.
    extras = {'.Shp': ('.Shx', '.Dbf', '.Prj'), '.SHP': ('.SHX', '.DBF', '.PRJ')}

    if ext in extras:
        for other_ext in extras[ext]:
            if os.path.exists(base + other_ext):
                os.link(base + other_ext, base + other_ext.lower())

    return normal_path

# TODO rip out a bunch of this and replace with call to row_extract_and_reproject
def ogr_source_to_csv(source_config, source_path, dest_path):
    ''' Convert a single shapefile or GeoJSON in source_path and put it in dest_path
    '''
    in_datasource = ogr.Open(source_path, 0)
    layer_id = source_config.data_source['conform'].get('layer', 0)
    if isinstance(layer_id, int):
        in_layer = in_datasource.GetLayerByIndex(layer_id)
        _L.info("Converting layer %s (%s) to CSV", layer_id, repr(in_layer.GetName()))
    else:
        in_layer = in_datasource.GetLayerByName(layer_id)
        _L.info("Converting layer %s to CSV", repr(in_layer.GetName()))

    # Determine the appropriate SRS
    inSpatialRef = in_layer.GetSpatialRef()
    srs = source_config.data_source["conform"].get("srs", None)

    # Skip Transformation is the EPSG code is superfluous
    if srs is not None:
        # OGR may have a projection, but use the explicit SRS instead
        if srs.startswith(u"EPSG:"):
            _L.debug("SRS tag found specifying %s", srs)
            inSpatialRef = osr.SpatialReference()
            inSpatialRef.ImportFromEPSG(int(srs[5:]))
        else:
            # OGR is capable of doing more than EPSG, but so far we don't need it.
            raise Exception("Bad SRS. Can only handle EPSG, the SRS tag is %s", srs)
    elif inSpatialRef is None:
        raise Exception("No projection found for source {}".format(source_path))

    # Determine the appropriate text encoding. This is complicated in OGR, see
    # https://github.com/openaddresses/machine/issues/42
    if in_layer.TestCapability(ogr.OLCStringsAsUTF8):
        # OGR turned this to UTF 8 for us
        shp_encoding = 'utf-8'
    elif "encoding" in source_config.data_source["conform"]:
        shp_encoding = source_config.data_source["conform"]["encoding"]
    else:
        _L.warning("No encoding given and OGR couldn't guess. Trying ISO-8859-1, YOLO!")
        shp_encoding = "iso-8859-1"
    _L.debug("Assuming shapefile data is encoded %s", shp_encoding)

    # Get the input schema, create an output schema
    in_layer_defn = in_layer.GetLayerDefn()
    out_fieldnames = []
    for i in range(0, in_layer_defn.GetFieldCount()):
        field_defn = in_layer_defn.GetFieldDefn(i)
        out_fieldnames.append(field_defn.GetName())
    out_fieldnames.append(GEOM_FIELDNAME)

    # Set up a transformation from the source SRS to EPSG:4326
    outSpatialRef = osr.SpatialReference()
    outSpatialRef.ImportFromEPSG(4326)

    if int(osgeo.__version__[0]) >= 3:
        # GDAL 3 changes axis order: https://github.com/OSGeo/gdal/issues/1546
        outSpatialRef.SetAxisMappingStrategy(osgeo.osr.OAMS_TRADITIONAL_GIS_ORDER)

    coordTransform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef)

    # Write a CSV file with one row per feature in the OGR source
    with open(dest_path, 'w', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=out_fieldnames)
        writer.writeheader()

        in_feature = in_layer.GetNextFeature()
        while in_feature:
            row = dict()

            for i in range(0, in_layer_defn.GetFieldCount()):
                field_defn = in_layer_defn.GetFieldDefn(i)
                field_value = in_feature.GetField(i)
                if field_defn.type is ogr.OFTString:
                    # Convert OGR's byte sequence strings to Python Unicode strings
                    field_value = in_feature.GetFieldAsBinary(i).decode(shp_encoding)
                row[field_defn.GetNameRef()] = field_value
            geom = in_feature.GetGeometryRef()
            if geom is not None:
                geom.Transform(coordTransform)

                if source_config.layer == "addresses":
                    # For Addresses - Calculate the centroid on surface of the geometry and write it as X and Y columns
                    try:
                        centroid = geom.PointOnSurface()
                    except RuntimeError as e:
                        if 'Invalid number of points in LinearRing found' not in str(e):
                            raise
                        xmin, xmax, ymin, ymax = geom.GetEnvelope()

                        centroid = ogr.CreateGeometryFromWkt("POINT ({} {})".format(xmin/2 + xmax/2, ymin/2 + ymax/2))

                    row[GEOM_FIELDNAME] = centroid.ExportToWkt()
                else:
                    row[GEOM_FIELDNAME] = geom.ExportToWkt()
            else:
                row[GEOM_FIELDNAME] = None

            writer.writerow(row)

            in_feature.Destroy()
            in_feature = in_layer.GetNextFeature()

    in_datasource.Destroy()

def csv_source_to_csv(source_config, source_path, dest_path):
    "Convert a source CSV file to an intermediate form, coerced to UTF-8 and EPSG:4326"
    _L.info("Converting source CSV %s", source_path)

    # Encoding processing tag
    enc = source_config.data_source["conform"].get("encoding", "utf-8")

    # csvsplit processing tag
    delim = source_config.data_source["conform"].get("csvsplit", ",")

    # Extract the source CSV, applying conversions to deal with oddball CSV formats
    # Also convert encoding to utf-8 and reproject to EPSG:4326 in X and Y columns
    with open(source_path, 'r', encoding=enc) as source_fp:
        in_fieldnames = None   # in most cases, we let the csv module figure these out

        # headers processing tag
        if "headers" in source_config.data_source["conform"]:
            headers = source_config.data_source["conform"]["headers"]
            if (headers == -1):
                # Read a row off the file to see how many columns it has
                temp_reader = csv.reader(source_fp, delimiter=str(delim))
                first_row = next(temp_reader)
                num_columns = len(first_row)
                source_fp.seek(0)
                in_fieldnames = ["COLUMN%d" % n for n in range(1, num_columns+1)]
                _L.debug("Synthesized header %s", in_fieldnames)
            else:
                # partial implementation of headers and skiplines,
                # matches the sources in our collection as of January 2015
                # this code handles the case for Korean inputs where there are
                # two lines of headers and we want to skip the first one
                assert "skiplines" in source_config.data_source["conform"]
                assert source_config.data_source["conform"]["skiplines"] == headers
                # Skip N lines to get to the real header. headers=2 means we skip one line
                for n in range(1, headers):
                    next(source_fp)
        else:
            # check the source doesn't specify skiplines without headers
            assert "skiplines" not in source_config.data_source["conform"]

        reader = csv.DictReader(source_fp, delimiter=delim, fieldnames=in_fieldnames)
        num_fields = len(reader.fieldnames)

        protocol_string = source_config.data_source['protocol']

        # Construct headers for the extracted CSV file
        if protocol_string == "ESRI":
            # ESRI sources: just copy what the downloader gave us. (Already has OA:GEOM)
            out_fieldnames = list(reader.fieldnames)

            out_fieldnames = list(map(lambda f: (
                GEOM_FIELDNAME if f == "OA:geom" else f
            ), out_fieldnames))

        else:
            # CSV sources: replace the source's lat/lon columns with OA:GEOM
            old_ll = [
                source_config.data_source["conform"]["lon"],
                source_config.data_source["conform"]["lat"]
            ]

            old_ll.extend([s.upper() for s in old_ll])
            out_fieldnames = [fn for fn in reader.fieldnames if fn not in old_ll]
            out_fieldnames.append(GEOM_FIELDNAME)

        # Write the extracted CSV file
        with open(dest_path, 'w', encoding='utf-8') as dest_fp:
            writer = csv.DictWriter(dest_fp, out_fieldnames)
            writer.writeheader()
            # For every row in the source CSV
            row_number = 0
            for source_row in reader:
                row_number += 1
                if len(source_row) != num_fields:
                    _L.debug("Skipping row. Got %d columns, expected %d", len(source_row), num_fields)
                    continue
                try:
                    out_row = row_extract_and_reproject(source_config, source_row)
                except Exception as e:
                    _L.error('Error in row {}: {}'.format(row_number, e))
                    raise
                else:
                    writer.writerow(out_row)

def geojson_source_to_csv(source_config, source_path, dest_path):
    '''
    '''
    # For every row in the source GeoJSON
    with open(source_path) as file:
        # Write the extracted CSV file
        with open(dest_path, 'w', encoding='utf-8') as dest_fp:
            writer = None
            for (row_number, feature) in enumerate(stream_geojson(file)):
                if writer is None:
                    out_fieldnames = list(feature['properties'].keys())
                    out_fieldnames.append(GEOM_FIELDNAME)
                    writer = csv.DictWriter(dest_fp, out_fieldnames)
                    writer.writeheader()

                try:
                    row = feature['properties']
                    if feature['geometry'] is None:
                        continue
                    geom = ogr.CreateGeometryFromJson(json.dumps(feature['geometry']))
                    if not geom:
                        continue

                    if source_config.layer == "addresses":
                        # For Addresses - Calculate the centroid on surface of the geometry and write it as X and Y columns
                        geom = geom.PointOnSurface()

                except Exception as e:
                    _L.error('Error in row {}: {}'.format(row_number, e))
                    raise
                else:
                    row.update({GEOM_FIELDNAME: geom.ExportToWkt()})
                    writer.writerow(row)

_transform_cache = {}
def _transform_to_4326(srs):
    "Given a string like EPSG:2913, return an OGR transform object to turn it in to EPSG:4326"
    if srs not in _transform_cache:
        epsg_id = int(srs[5:]) if srs.startswith("EPSG:") else int(srs)
        # Manufacture a transform object if it's not in the cache

        in_spatial_ref = osr.SpatialReference()
        in_spatial_ref.ImportFromEPSG(epsg_id)

        out_spatial_ref = osr.SpatialReference()
        out_spatial_ref.ImportFromEPSG(4326)

        if int(osgeo.__version__[0]) >= 3:
            # GDAL 3 changes axis order: https://github.com/OSGeo/gdal/issues/1546
            in_spatial_ref.SetAxisMappingStrategy(osgeo.osr.OAMS_TRADITIONAL_GIS_ORDER)
            out_spatial_ref.SetAxisMappingStrategy(osgeo.osr.OAMS_TRADITIONAL_GIS_ORDER)

        _transform_cache[srs] = osr.CoordinateTransformation(in_spatial_ref, out_spatial_ref)
    return _transform_cache[srs]

def row_extract_and_reproject(source_config, source_row):
    ''' Find geometries in source CSV data and store it in ESPG:4326
    '''
    data_source = source_config.data_source

    format_string = data_source["conform"].get('format')
    protocol_string = data_source['protocol']

    # Prepare an output row
    out_row = copy.deepcopy(source_row)

    source_geom = None

    # Set local variables lon_name, source_x, lat_name, source_y
    if source_row.get(GEOM_FIELDNAME) is not None:
        source_geom = source_row[GEOM_FIELDNAME]
    elif source_row.get(GEOM_FIELDNAME.replace('GEOM', 'geom')):
        source_row[GEOM_FIELDNAME] = source_row[GEOM_FIELDNAME.replace('GEOM', 'geom')]
        source_geom = source_row[GEOM_FIELDNAME]

    if source_row.get(GEOM_FIELDNAME.replace('GEOM', 'geom')) is not None:
        del out_row[GEOM_FIELDNAME.replace('GEOM', 'geom')]

    if source_geom == "POINT (nan nan)":
        out_row[GEOM_FIELDNAME] = None
        return out_row

    if source_geom is None and data_source["conform"].get('lat') is not None and data_source["conform"].get('lon') is not None:
        # Conforms can name the lat/lon columns from the original source data
        lat_name = data_source["conform"]["lat"]
        lon_name = data_source["conform"]["lon"]

        if lon_name in source_row:
            source_x = source_row[lon_name]
        else:
            source_x = source_row[lon_name.upper()]

        if lat_name in source_row:
            source_y = source_row[lat_name]
        else:
            source_y = source_row[lat_name.upper()]

        # Remove lat/lng name from output row
        for n in lon_name, lon_name.upper(), lat_name, lat_name.upper():
            if n in out_row: del out_row[n]

        # Convert commas to periods for decimal numbers. (Not using locale.)
        try:
            source_x = source_x.replace(',', '.')
            source_y = source_y.replace(',', '.')

            source_geom = "POINT ({} {})".format(source_x, source_y)

            if source_x == "" or source_y == "":
                out_row[GEOM_FIELDNAME] = None
                return out_row
        except AttributeError:
            # Add blank data to the output CSV and get out
            out_row[GEOM_FIELDNAME] = None
            return out_row


    # Reproject the coordinates if necessary
    if "srs" in data_source["conform"] and data_source["conform"]["srs"] != "EPSG:4326":
        try:
            srs = data_source["conform"]["srs"]
            geom = ogr.CreateGeometryFromWkt(source_geom)
            geom.Transform(_transform_to_4326(srs))
            source_geom = geom.ExportToWkt()
        except (TypeError, ValueError) as e:
            if not (source_x == "" or source_y == ""):
                _L.debug("Could not reproject %s %s in SRS %s", source_x, source_y, srs)

    # For Addresses - Calculate the centroid on surface of the geometry and write it as X and Y columns
    if source_config.layer == "addresses":
        geom = ogr.CreateGeometryFromWkt(source_geom)

        try:
            centroid = geom.PointOnSurface()
        except RuntimeError as e:
            if 'Invalid number of points in LinearRing found' not in str(e):
                raise
            xmin, xmax, ymin, ymax = geom.GetEnvelope()

            centroid = ogr.CreateGeometryFromWkt("POINT ({} {})".format(xmin/2 + xmax/2, ymin/2 + ymax/2))

        source_geom = centroid.ExportToWkt()

    out_row[GEOM_FIELDNAME] = source_geom

    # Add the reprojected data to the output CSV
    return out_row


def row_function(sc, row, key, fxn):
    function = fxn["function"]
    if function == "join":
        row = row_fxn_join(sc, row, key, fxn)
    elif function == "regexp":
        row = row_fxn_regexp(sc, row, key, fxn)
    elif function == "format":
        row = row_fxn_format(sc, row, key, fxn)
    elif function == "prefixed_number":
        row = row_fxn_prefixed_number(sc, row, key, fxn)
    elif function == "postfixed_street":
        row = row_fxn_postfixed_street(sc, row, key, fxn)
    elif function == "postfixed_unit":
        row = row_fxn_postfixed_unit(sc, row, key, fxn)
    elif function == "remove_prefix":
        row = row_fxn_remove_prefix(sc, row, key, fxn)
    elif function == "remove_postfix":
        row = row_fxn_remove_postfix(sc, row, key, fxn)
    elif function == "chain":
        row = row_fxn_chain(sc, row, key, fxn)
    elif function == "first_non_empty":
        row = row_fxn_first_non_empty(sc, row, key, fxn)

    return row



### Row-level conform code. Inputs and outputs are individual rows in a CSV file.
### The input row may or may not be modified in place. The output row is always returned.
def row_transform_and_convert(source_config, row):
    "Apply the full conform transform and extract operations to a row"

    # Some conform specs have fields named with a case different from the source
    row = row_smash_case(source_config.data_source, row)

    c = source_config.data_source["conform"]

    "Attribute tags can utilize processing fxns"
    for k, v in c.items():
        if k.upper() in source_config.SCHEMA and type(v) is list:
            "Lists are a concat shortcut to concat fields with spaces"
            row = row_merge(source_config, row, k)
        if k.upper() in source_config.SCHEMA and type(v) is dict:
            "Dicts are custom processing functions"
            row = row_function(source_config, row, k, v)

    # Make up a random fingerprint if none exists
    cache_fingerprint = source_config.data_source.get('fingerprint', str(uuid4()))

    row = row_convert_to_out(source_config, row)

    if source_config.layer == "addresses":
        row = row_canonicalize_unit_and_number(source_config.data_source, row)
        row = row_round_lat_lon(source_config.data_source, row)

    row = row_calculate_hash(cache_fingerprint, row)
    return row

def fxn_smash_case(fxn):
    if "field" in fxn:
        fxn["field"] = fxn["field"].lower()
    if "fields" in fxn:
        fxn["fields"] = [s.lower() for s in fxn["fields"]]
    if "field_to_remove" in fxn:
        fxn["field_to_remove"] = fxn["field_to_remove"].lower()
    if "functions" in fxn:
        for sub_fxn in fxn["functions"]:
            fxn_smash_case(sub_fxn)

def conform_smash_case(data_source):
    "Convert all named fields in data_source object to lowercase. Returns new object."
    new_sd = copy.deepcopy(data_source)
    conform = new_sd["conform"]

    for k, v in conform.items():
        if type(conform[k]) is str and k.upper() in RESERVED_SCHEMA:
            conform[k] = v.lower()
        if type(conform[k]) is list:
            conform[k] = [s.lower() for s in conform[k]]
        if type(conform[k]) is dict:
            fxn_smash_case(conform[k])

            if "functions" in conform[k] and type(conform[k]["functions"]) is list:
                for function in conform[k]["functions"]:
                    if type(function) is dict:
                        if "field" in function:
                            function["field"] = function["field"].lower()

                        if "fields" in function:
                            function["fields"] = [s.lower() for s in function["fields"]]

                        if "field_to_remove" in function:
                            function["field_to_remove"] = function["field_to_remove"].lower()

    return new_sd

def row_smash_case(sc, input):
    "Convert all field names to lowercase. Slow, but necessary for imprecise conform specs."
    output = { k.lower() : v for (k, v) in input.items() }
    return output

def row_merge(sc, row, key):
    "Merge multiple columns like 'Maple','St' to 'Maple St'"
    merge_data = [row[field] for field in sc.data_source["conform"][key]]
    row["oa:{}".format(key)] = ' '.join(merge_data)
    return row

def row_fxn_join(sc, row, key, fxn):
    "Create new columns by merging arbitrary other columns with a separator"
    separator = fxn.get("separator", " ")
    try:
        fields = [(row[n] or u'').strip() for n in fxn["fields"]]
        row["oa:{}".format(key)] = separator.join([f for f in fields if f])
    except Exception as e:
        _L.debug("Failure to merge row %r %s", e, row)
    return row

def row_fxn_regexp(sc, row, key, fxn):
    "Split addresses like '123 Maple St' into '123' and 'Maple St'"
    pattern = re.compile(fxn.get("pattern", False))
    replace = fxn.get('replace', False)
    if replace:
        match = re.sub(pattern, convert_regexp_replace(replace), row[fxn["field"]])
        row["oa:{}".format(key)] = match;
    else:
        match = pattern.search(row[fxn["field"]])
        row["oa:{}".format(key)] = ''.join(match.groups()) if match else '';
    return row

def row_fxn_prefixed_number(sc, row, key, fxn):
    "Extract '123' from '123 Maple St'"

    match = prefixed_number_pattern.search(row[fxn["field"]])
    row["oa:{}".format(key)] = ''.join(match.groups()) if match else '';

    return row

def row_fxn_postfixed_street(sc, row, key, fxn):
    "Extract 'Maple St' from '123 Maple St'"

    may_contain_units = fxn.get('may_contain_units', False)

    if may_contain_units:
        match = postfixed_street_with_units_pattern.search(row[fxn["field"]])
    else:
        match = postfixed_street_pattern.search(row[fxn["field"]])

    row["oa:{}".format(key)] = ''.join(match.groups()) if match else '';

    return row

def row_fxn_postfixed_unit(sc, row, key, fxn):
    "Extract 'Suite 300' from '123 Maple St Suite 300'"

    match = postfixed_unit_pattern.search(row[fxn["field"]])
    row["oa:{}".format(key)] = ''.join(match.groups()) if match else '';

    return row

def row_fxn_remove_prefix(sc, row, key, fxn):
    "Remove a 'field_to_remove' from the beginning of 'field' if it is a prefix"
    if row[fxn["field"]].startswith(row[fxn["field_to_remove"]]):
        row["oa:{}".format(key)] = row[fxn["field"]][len(row[fxn["field_to_remove"]]):].lstrip(' ')
    else:
        row["oa:{}".format(key)] = row[fxn["field"]]

    return row

def row_fxn_remove_postfix(sc, row, key, fxn):
    "Remove a 'field_to_remove' from the end of 'field' if it is a postfix"
    if row[fxn["field_to_remove"]] != "" and row[fxn["field"]].endswith(row[fxn["field_to_remove"]]):
        row["oa:{}".format(key)] = row[fxn["field"]][0:len(row[fxn["field_to_remove"]])*-1].rstrip(' ')
    else:
        row["oa:{}".format(key)] = row[fxn["field"]]

    return row

def row_fxn_format(sc, row, key, fxn):
    "Format multiple fields using a user-specified format string"
    format_var_pattern = re.compile('\$([0-9]+)')

    fields = [(row[n] or u'').strip() for n in fxn["fields"]]

    parts = []

    idx = 0
    num_fields_added = 0

    format_str = fxn["format"]
    for i, m in enumerate(format_var_pattern.finditer(format_str)):
        field_idx = int(m.group(1))
        start, end = m.span()

        if field_idx > 0 and field_idx - 1 < len(fields):
            field = fields[field_idx - 1]

            if idx == 0 or (num_fields_added > 0 and field):
                parts.append(format_str[idx:start])

            if field:
                # if the value being added ends with '.0', remove it
                # certain fields ending with '.0' are normalized by removing that
                #  suffix in row_canonicalize_unit_and_number but this isn't
                #  possible when not-the-last component fields submitted to the format
                #  function end with '.0'
                if field.endswith(".0"):
                    field = field[:-2]

                parts.append(field)
                num_fields_added += 1

        idx = end

    if num_fields_added > 0:
        parts.append(format_str[idx:])
        row["oa:{}".format(key)] = u''.join(parts)
    else:
        row["oa:{}".format(key)] = u''

    return row

def row_fxn_chain(sc, row, key, fxn):
    functions = fxn["functions"]
    var = fxn.get("variable")

    original_key = key

    if var and var.upper().lstrip('OA:') not in sc.SCHEMA and var not in row:
        row['oa:' + var] = u''
        key = var

    for func in functions:
        row = row_function(sc, row, key, func)

        if row.get('oa:' + key):
            row[key] = row['oa:' + key]

    row['oa:{}'.format(original_key.lower())] = row['oa:{}'.format(key)]

    return row

def row_fxn_first_non_empty(sc, row, key, fxn):
    "Iterate all fields looking for first that has a non-empty value"
    for field in fxn.get('fields', []):
        if row[field] and row[field].strip():
            row["oa:{}".format(key)] = row[field]
            break

    return row

def row_canonicalize_unit_and_number(sc, row):
    "Canonicalize address unit and number"
    row["UNIT"] = (row["UNIT"] or '').strip()
    row["NUMBER"] = (row["NUMBER"] or '').strip()
    if row["NUMBER"].endswith(".0"):
        row["NUMBER"] = row["NUMBER"][:-2]
    row["STREET"] = (row["STREET"] or '').strip()
    return row

def _round_wgs84_to_7(n):
    "Round a WGS84 coordinate to 7 decimal points. Input and output both strings."
    try:
        return "%.12g" % round(float(n), 7)
    except:
        return n

def row_round_lat_lon(sc, row):
    "Round WGS84 coordinates to 1cm precision"
    if row.get('GEOM') is not None and 'POINT' in row['GEOM']:
        try:
            geom = ogr.CreateGeometryFromWkt(row['GEOM'])
            x = _round_wgs84_to_7(geom.GetX())
            y = _round_wgs84_to_7(geom.GetY())

            row['GEOM'] = ogr.CreateGeometryFromWkt('POINT ({} {})'.format(x, y)).ExportToWkt()
        except Exception:
            pass

    return row

def row_calculate_hash(cache_fingerprint, row):
    ''' Calculate row hash based on content and existing fingerprint.

        16 chars of SHA-1 gives a 64-bit value, plenty for all addresses.
    '''
    hash = sha1(cache_fingerprint.encode('utf8'))
    hash.update(json.dumps(sorted(row.items()), separators=(',', ':')).encode('utf8'))
    row.update(HASH=hash.hexdigest()[:16])

    return row

def row_convert_to_out(source_config, row):
    "Convert a row from the source schema to OpenAddresses output schema"

    output = {
        "GEOM": row.get(GEOM_FIELDNAME.lower(), None),
    }

    for field in source_config.SCHEMA:
        if row.get('oa:{}'.format(field.lower())) is not None:
            # If there is an OA prefix, it is not a native field and was compiled
            # via an attrib funciton or concatentation
            output[field] = row.get('oa:{}'.format(field.lower()))
        else:
            # Get a native field as specified in the conform object
            cfield = source_config.data_source['conform'].get(field.lower())
            if cfield:
                output[field] = row.get(cfield.lower())
            else:
                output[field] = ''

    return output

### File-level conform code. Inputs and outputs are filenames.

def extract_to_source_csv(source_config, source_path, extract_path):
    """Extract arbitrary downloaded sources to an extracted CSV in the source schema.
    source_config: description of the source, containing the conform object
    extract_path: file to write the extracted CSV file

    The extracted file will be in UTF-8 and will have X and Y columns corresponding
    to longitude and latitude in EPSG:4326.
    """
    format_string = source_config.data_source["conform"]['format']
    protocol_string = source_config.data_source['protocol']

    if format_string in ("shapefile", "xml", "gdb"):
        ogr_source_path = normalize_ogr_filename_case(source_path)
        ogr_source_to_csv(source_config, ogr_source_path, extract_path)
    elif format_string == "csv":
        csv_source_to_csv(source_config, source_path, extract_path)
    elif format_string == "geojson":
        # GeoJSON sources have some awkward legacy with ESRI, see issue #34
        if protocol_string == "ESRI":
            _L.info("ESRI GeoJSON source found; treating it as CSV")
            csv_source_to_csv(source_config, source_path, extract_path)
        else:
            _L.info("Non-ESRI GeoJSON source found; converting as a stream.")
            geojson_source_path = normalize_ogr_filename_case(source_path)
            geojson_source_to_csv(source_config, geojson_source_path, extract_path)
    else:
        raise Exception("Unsupported source format %s" % format_string)

def transform_to_out_csv(source_config, extract_path, dest_path):
    ''' Transform an extracted source CSV to the OpenAddresses output CSV by applying conform rules.

        source_config: description of the source, containing the conform object
        extract_path: extracted CSV file to process
        dest_path: path for output file in OpenAddress CSV
    '''
    # Convert all field names in the conform spec to lower case
    source_config.data_source = conform_smash_case(source_config.data_source)

    # Read through the extract CSV
    with open(extract_path, 'r', encoding='utf-8') as extract_fp:
        reader = csv.DictReader(extract_fp)
        # Write to the destination CSV
        with open(dest_path, 'w', encoding='utf-8') as dest_fp:
            writer = csv.DictWriter(dest_fp, ['GEOM', 'HASH', *source_config.SCHEMA])
            writer.writeheader()
            # For every row in the extract
            for extract_row in reader:
                out_row = row_transform_and_convert(source_config, extract_row)
                writer.writerow(out_row)

def conform_cli(source_config, source_path, dest_path):
    "Command line entry point for conforming a downloaded source to an output CSV."
    # TODO: this tool only works if the source creates a single output

    if "conform" not in source_config.data_source:
        return 1

    format_string = source_config.data_source["conform"].get('format')

    if not format_string in ["shapefile", "geojson", "csv", "xml", "gdb"]:
        _L.warning("Skipping file with unknown conform: %s", source_path)
        return 1

    # Create a temporary filename for the intermediate extracted source CSV
    fd, extract_path = tempfile.mkstemp(prefix='openaddr-extracted-', suffix='.csv')
    os.close(fd)
    _L.debug('extract temp file %s', extract_path)

    try:
        extract_to_source_csv(source_config, source_path, extract_path)
        transform_to_out_csv(source_config, extract_path, dest_path)
    finally:
        os.remove(extract_path)

    return 0

def check_source_tests(source_config):
    ''' Return boolean status and a message if any tests failed.
    '''
    try:
        # Convert all field names in the conform spec to lower case
        source_config.data_source = conform_smash_case(source_config.data_source)
    except:
        # There may be problems in the source spec - ignore them for now.
         source_config.data_source = source_config.data_source

    source_test = source_config.data_source.get('test', {})
    tests_enabled = source_test.get('enabled', True)
    acceptance_tests = source_test.get('acceptance-tests')

    if not tests_enabled or not acceptance_tests:
        # There is nothing to be done here.
        return None, None

    for (index, test) in enumerate(acceptance_tests):
        input = row_smash_case(source_config.data_source, test['inputs'])
        output = row_smash_case(source_config.data_source, row_transform_and_convert(source_config, input))
        actual = {k: v for (k, v) in output.items() if k in test['expected']}
        expected = row_smash_case(source_config.data_source, test['expected'])

        if actual != expected:
            expected_json = json.dumps(expected, ensure_ascii=False)
            actual_json = json.dumps(actual, ensure_ascii=False)
            description = test.get('description', 'test {}'.format(index))
            return False, 'For {}, expected {} but got {}'.format(description, expected_json, actual_json)

    # Yay, everything passed.
    return True, None

import logging; _L = logging.getLogger('openaddr.preview')

from tempfile import mkstemp
from math import pow, sqrt, pi, log
from argparse import ArgumentParser
import json, itertools, os, struct

import requests, uritemplate, mapbox_vector_tile

from osgeo import osr, ogr

try:
    import cairo
except ImportError:
    # http://stackoverflow.com/questions/11491268/install-pycairo-in-virtualenv
    import cairocffi as cairo

TILE_URL = 'http://a.tiles.mapbox.com/v4/mapbox.mapbox-streets-v7/{z}/{x}/{y}.mvt{?access_token}'
EARTH_DIAMETER = 6378137 * 2 * pi
FORMAT = 'ff'

# WGS 84, http://spatialreference.org/ref/epsg/4326/
EPSG4326 = '+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs'

# Web Mercator, https://trac.osgeo.org/openlayers/wiki/SphericalMercator
EPSG900913 = '+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +no_defs'

def render(src_filename, png_filename, width, resolution, mapbox_key):
    '''
    '''
    try:
        xmin, ymin, xmax, ymax = calculate_bounds(src_filename)
    except:
        raise

    surface, context, scale = make_context(xmin, ymin, xmax, ymax, width, resolution)

    _L.info('Preview width {:.0f}, scale {:.5f}, zoom {:.2f}'.format(width, scale, calculate_zoom(scale, resolution)))

    # Map units per reference pixel (http://www.w3.org/TR/css3-values/#reference-pixel)
    muppx = resolution / scale


    black = 0x00, 0x00, 0x00
    off_white = 0xFF/0xFF, 0xFC/0xFF, 0xF9/0xFF
    water_fill = 0xC7/0xFF, 0xDE/0xFF, 0xF5/0xFF # 0xDD/0xFF, 0xEA/0xFF, 0xF8/0xFF
    point_fill = 0x74/0xFF, 0xA5/0xFF, 0x78/0xFF
    road_stroke = 0xC0/0xFF, 0xE0/0xFF, 0xE0/0xFF # 0xE0/0xFF, 0xE3/0xFF, 0xE5/0xFF
    park_fill = 0xDD/0xFF, 0xF6/0xFF, 0xDE/0xFF
    orange_over_land = 0xFE/0xFF, 0xCB/0xFF, 0x9F/0xFF
    orange_over_water = 0xE2/0xFF, 0xBB/0xFF, 0x9A/0xFF
    orange_darker = 0xFE/0xFF, 0x96/0xFF, 0x3F/0xFF, 0.5

    context.set_source_rgb(*off_white)
    context.rectangle(xmin, ymax, xmax - xmin, ymin - ymax)
    context.fill()

    landuse_geoms, water_geoms, roads_geoms = \
        get_map_features(xmin, ymin, xmax, ymax, resolution, scale, mapbox_key)

    fill_geometries(context, landuse_geoms, muppx, park_fill)
    fill_geometries(context, water_geoms, muppx, water_fill)

    context.set_line_width(.5 * muppx)
    context.set_source_rgb(*road_stroke)
    stroke_geometries(context, roads_geoms)

    context.set_line_width(.25 * muppx)

    for geom in iterate_file_geoms(src_filename):
        if geom.GetGeometryType() == ogr.wkbMultiLineString or geom.GetGeometryType() == ogr.wkbLineString:
            stroke_geometries(context, [geom])
        elif geom.GetGeometryType() == ogr.wkbMultiPolygon or geom.GetGeometryType() == ogr.wkbPolygon:
            fill_geometries(context, [geom], muppx, black)
        else:
            (x, y, e) = geom.PointOnSurface().GetPoint()

            context.arc(x, y, 15, 0, 2 * pi)
            context.set_source_rgb(*point_fill)
            context.fill()
            context.arc(x, y, 15, 0, 2 * pi)
            context.set_source_rgb(*black)
            context.stroke()
            (x, y, e) = geom.PointOnSurface().GetPoint()

    surface.write_to_png(png_filename)

def iterate_file_geoms(filename):
    ''' Stream Geometries from an input GeoJSON+LD File
    '''

    with open(filename, 'r') as file:
        project = get_projection()

        for line in file:
            try:
                line = json.loads(line)

                geom = ogr.CreateGeometryFromJson(json.dumps(line['geometry']))

                geom.Transform(project)

                yield geom
            except Exception as e:
                print('ERROR', e)
                continue

    del project, geom

def get_map_features(xmin, ymin, xmax, ymax, resolution, scale, mapbox_key):
    '''
    '''
    zoom = round(calculate_zoom(scale, resolution))
    mincol = 2**zoom * (xmin + EARTH_DIAMETER/2) / EARTH_DIAMETER
    minrow = 2**zoom * (EARTH_DIAMETER/2 - ymax) / EARTH_DIAMETER
    maxcol = 2**zoom * (xmax + EARTH_DIAMETER/2) / EARTH_DIAMETER
    maxrow = 2**zoom * (EARTH_DIAMETER/2 - ymin) / EARTH_DIAMETER

    row_cols = itertools.product(range(int(minrow), int(maxrow) + 1),
                                 range(int(mincol), int(maxcol) + 1))

    landuse_geoms, water_geoms, roads_geoms = list(), list(), list()

    def tile_bounds(row, col, zoom):
        ''' Get Mercator points for corners of this tile.
        '''
        ulx = EARTH_DIAMETER * (col / 2**zoom - 1/2)
        uly = EARTH_DIAMETER * (1/2 - row / 2**zoom)
        lrx = EARTH_DIAMETER * ((col + 1) / 2**zoom - 1/2)
        lry = EARTH_DIAMETER * (1/2 - (row + 1) / 2**zoom)
        return ulx, uly, lrx, lry

    def get_transform(extent, xmin, uly, lrx, lry):
        ''' Get scale and offset coefficients for tile coordinates.
        '''
        mx, bx = (lrx - xmin) / extent, xmin
        my, by = (uly - lry) / extent, lry
        return mx, bx, my, by

    def projected_geom(geometry, mx, bx, my, by):
        ''' Get an OGR geometry for a tiled GeoJSON-like geometry.
        '''
        if geometry['type'] in ('MultiPolygon', ):
            coordinates = [[[(mx * x + bx, my * y + by)
                for (x, y) in ring] for ring in part] for part in geometry['coordinates']]
        elif geometry['type'] in ('Polygon', 'MultiLineString'):
            coordinates = [[(mx * x + bx, my * y + by)
                for (x, y) in part] for part in geometry['coordinates']]
        elif geometry['type'] in ('LineString'):
            coordinates = [(mx * x + bx, my * y + by)
                for (x, y) in geometry['coordinates']]
        else:
            raise ValueError(geometry['type'])
        geom = ogr.CreateGeometryFromJson(json.dumps(dict(type=geometry['type'], coordinates=coordinates)))
        return geom

    for (row, col) in row_cols:
        url = uritemplate.expand(TILE_URL, dict(z=zoom, x=col, y=row, access_token=mapbox_key))

        _L.debug('Getting tile {}'.format(url))

        got = requests.get(url)
        tile = mapbox_vector_tile.decode(got.content)
        bounds = tile_bounds(row, col, zoom)

        if 'landuse' in tile:
            landuse_xform = get_transform(tile['landuse']['extent'], *bounds)
            for feature in tile['landuse']['features']:
                if 'Polygon' in feature['geometry']['type']:
                    if feature['properties'].get('class') in ('cemetery', 'forest', 'golf_course', 'grave_yard', 'meadow', 'park', 'pitch', 'wood'):
                        landuse_geoms.append(projected_geom(feature['geometry'], *landuse_xform))

        if 'water' in tile:
            water_xform = get_transform(tile['water']['extent'], *bounds)
            for feature in tile['water']['features']:
                if 'Polygon' in feature['geometry']['type']:
                    water_geoms.append(projected_geom(feature['geometry'], *water_xform))

        if 'road' in tile:
            road_xform = get_transform(tile['road']['extent'], *bounds)
            for feature in tile['road']['features']:
                if 'LineString' in feature['geometry']['type']:
                    if feature['properties'].get('class') in ('motorway', 'motorway_link', 'trunk', 'primary', 'secondary', 'tertiary', 'link', 'street', 'street_limited', 'pedestrian', 'construction', 'track', 'service', 'major_rail', 'minor_rail'):
                        roads_geoms.append(projected_geom(feature['geometry'], *road_xform))

    return landuse_geoms, water_geoms, roads_geoms

def get_projection():
    '''
    '''
    osr.UseExceptions()
    sref_geo = osr.SpatialReference(); sref_geo.ImportFromProj4(EPSG4326)
    sref_map = osr.SpatialReference(); sref_map.ImportFromProj4(EPSG900913)
    return osr.CoordinateTransformation(sref_geo, sref_map)

def write_geoms(geoms, geoms_filename):
    ''' Write a stream of geoms into a file of packed values.
    '''
    count = 0

    with open(geoms_filename, mode='wb') as file:
        for geom in geoms:
            file.write(geom)
            count += 1

    _L.info('Wrote {} points to {}'.format(count, geoms_filename))

def stats(geoms_filename):
    ''' Return means and standard deviations for iterator geoms

        Uses Welford's numerically stable algorithm from
        https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
    '''
    n, xmean, xM2, ymean, yM2 = 0, 0, 0, 0, 0

    for geom in iterate_file_geoms(geoms_filename):
        (x, y, e) = geom.PointOnSurface().GetPoint()

        n += 1

        xdelta = x - xmean
        xmean += xdelta / n
        xM2 += xdelta * (x - xmean)

        ydelta = y - ymean
        ymean += ydelta / n
        yM2 += ydelta * (y - ymean)

    if n < 2:
        raise ValueError()

    xstddev, ystddev = sqrt(xM2 / (n - 1)), sqrt(yM2 / (n - 1))

    return xmean, xstddev, ymean, ystddev

def calculate_zoom(scale, resolution):
    ''' Calculate web map zoom based on scale.
    '''
    scale_at_zero = resolution * 256 / EARTH_DIAMETER
    zoom = log(scale / scale_at_zero) / log(2)

    return zoom

def calculate_bounds(geoms_filename):
    '''
    '''
    xmean, xsdev, ymean, ysdev = stats(geoms_filename)

    # use standard deviation to avoid far-flung mistakes, and look further
    # horizontally to account for Github comment thread image appearance.
    xmin, xmax = xmean - 5 * xsdev, xmean + 5 * xsdev
    ymin, ymax = ymean - 3 * ysdev, ymean + 3 * ysdev

    # look at the actual points
    left, right = xmax, xmin
    bottom, top = ymax, ymin

    for geom in iterate_file_geoms(geoms_filename):
        (x, y, e) = geom.PointOnSurface().GetPoint()

        if xmin <= x <= xmax:
            left, right = min(left, x), max(right, x)
        if ymin <= y <= ymax:
            bottom, top = min(bottom, y), max(top, y)

    # pad by 2% on all sides
    width, height = right - left, top - bottom
    left -= width / 50
    bottom -= height / 50
    right += width / 50
    top += height / 50

    return left, bottom, right, top

def make_context(left, bottom, right, top, width=668, resolution=1):
    ''' Get Cairo surface, context, and drawing scale.

        668px is the width of a comment box in Github, one place where
        these previews are designed to be used.
    '''
    aspect = (right - left) / (top - bottom)

    hsize = int(resolution * width)
    vsize = int(hsize / aspect)

    hscale = hsize / (right - left)
    vscale = (hsize / aspect) / (bottom - top)

    hoffset = -left
    voffset = -top

    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, hsize, vsize)
    context = cairo.Context(surface)
    context.scale(hscale, vscale)
    context.translate(hoffset, voffset)

    return surface, context, hscale

def stroke_geometries(ctx, geometries):
    '''
    '''
    for geometry in geometries:
        if geometry.GetGeometryType() in (ogr.wkbMultiPolygon, ogr.wkbMultiLineString):
            parts = geometry
        elif geometry.GetGeometryType() in (ogr.wkbPolygon, ogr.wkbLineString):
            parts = [geometry]
        else:
            continue

        for part in parts:
            if part.GetGeometryType() is ogr.wkbPolygon:
                rings = part
            else:
                rings = [part]

            for ring in rings:
                points = ring.GetPoints()
                if geometry.GetGeometryType() in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
                    draw_line(ctx, points[-1], points)
                else:
                    draw_line(ctx, points[0], points[1:])
                ctx.stroke()

def fill_geometries(ctx, geometries, muppx, rgb):
    '''
    '''
    ctx.set_source_rgb(*rgb)

    for geometry in geometries:
        if geometry.GetGeometryType() == ogr.wkbMultiPolygon:
            parts = geometry
        elif geometry.GetGeometryType() == ogr.wkbPolygon:
            parts = [geometry]
        elif geometry.GetGeometryType() == ogr.wkbPoint:
            buffer = geometry.Buffer(2 * muppx, 3)
            parts = [buffer]
        else:
            raise NotImplementedError()

        for part in parts:
            for ring in part:
                points = ring.GetPoints()
                draw_line(ctx, points[-1], points)
            ctx.fill()

def draw_line(ctx, start, points):
    '''
    '''
    ctx.move_to(*start)

    for point in points:
        ctx.line_to(*point)

parser = ArgumentParser(description='Draw a map of a single source preview.')

parser.add_argument('src_geojson', help='Input GeoJSON')
parser.add_argument('png_filename', help='Output PNG filename.')

parser.set_defaults(resolution=1, width=668)

parser.add_argument('--2x', dest='resolution', action='store_const', const=2,
                    help='Draw at double resolution.')

parser.add_argument('--1x', dest='resolution', action='store_const', const=1,
                    help='Draw at normal resolution.')

parser.add_argument('--width', dest='width', type=int,
                    help='Width in pixels.')

parser.add_argument('--mapbox-key', dest='mapbox_key',
                    help='Mapbox API Key. See: https://mapbox.com/')

parser.add_argument('-v', '--verbose', help='Turn on verbose logging',
                    action='store_const', dest='loglevel',
                    const=logging.DEBUG, default=logging.INFO)

parser.add_argument('-q', '--quiet', help='Turn off most logging',
                    action='store_const', dest='loglevel',
                    const=logging.WARNING, default=logging.INFO)

def main():
    args = parser.parse_args()
    render(args.src_geojson, args.png_filename, args.width, args.resolution, args.mapbox_key)

if __name__ == '__main__':
    exit(main())

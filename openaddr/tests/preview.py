from __future__ import division

import os
import json
import unittest
import tempfile
import subprocess

from os.path import join, dirname
from shutil import rmtree

from httmock import HTTMock, response

from .. import preview

class TestPreview (unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix='TestPreview-')

    def tearDown(self):
        rmtree(self.temp_dir)

    def test_stats(self):
        points = [(-108 + (n * 0.001), -37 + (n * 0.001)) for n in range(0, 1000)]
        points_filename = join(self.temp_dir, 'points.geojson')

        with open(points_filename, 'w') as file:
            for point in points:
                file.write(json.dumps({
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Point",
                        "coordinates": point
                    }
                }) + '\n')

        xmean, xsdev, ymean, ysdev = preview.stats(points_filename)
        self.assertAlmostEqual(xmean, -11966900.920021897)
        self.assertAlmostEqual(xsdev, 32151.232557143696)

    def test_calculate_bounds(self):
        points = [(-108 + (n * 0.001), -37 + (n * 0.001)) for n in range(0, 1000)]
        points += [(-1, -1), (0, 0), (1, 1)]

        points_filename = join(self.temp_dir, 'points.geojson')

        with open(points_filename, 'w') as file:
            for point in points:
                file.write(json.dumps({
                    "type": "Feature",
                    "properties": {},
                    "geometry": {
                        "type": "Point",
                        "coordinates": point
                    }
                }) + '\n')

        bbox = preview.calculate_bounds(points_filename)
        self.assertEqual(bbox, (-12024729.169099594, -4441873.743568107, -11909072.670945017, -4297992.015057018), 'The two outliers are ignored')

    def test_render_geojson(self):
        '''
        '''
        def response_content(url, request):
            if url.hostname == 'api.protomaps.com' and url.path.startswith('/tiles/v3'):
                if 'access_token=protomaps-XXXX' not in url.query:
                    raise ValueError('Missing or wrong API key')
                data = b'\x1a\'x\x02\n\x05water(\x80 \x12\x19\x18\x03"\x13\t\xe0\x7f\xff\x1f\x1a\x00\xe0\x9f\x01\xdf\x9f\x01\x00\x00\xdf\x9f\x01\x0f\x08\x00'
                return response(200, data, headers={'Content-Type': 'application/vnd.mapbox-vector-tile'})
            raise Exception("Uknown URL")

        handle, png_filename = tempfile.mkstemp(prefix='render-', suffix='.png')
        os.close(handle)

        try:
            temp_dir = tempfile.mkdtemp(prefix='test_render_geojson-')

            with HTTMock(response_content):
                preview.render(join(dirname(__file__), 'outputs', 'denver-metro-preview.geojson'), png_filename, 668, 1, 'protomaps-XXXX')

            info = str(subprocess.check_output(('file', png_filename)))

            self.assertTrue('PNG image data' in info)
            self.assertTrue('668 x 493' in info)
            self.assertTrue('8-bit/color RGB' in info)
        finally:
            os.remove(png_filename)
            os.rmdir(temp_dir)

    def test_get_map_features(self):
        '''
        '''
        def response_content(url, request):
            if url.hostname == 'api.protomaps.com' and url.path.startswith('/tiles/v3'):
                if 'key=protomaps-XXXX' not in url.query:
                    raise ValueError('Missing or wrong API key')
                with open(join(dirname(__file__), 'data', 'mapbox-tile.mvt'), 'rb') as file:
                    data = file.read()
                return response(200, data, headers={'Content-Type': 'application/vnd.mapbox-vector-tile'})
            raise Exception("Uknown URL")

        xmin, ymin, xmax, ymax = -13611952, 4551290, -13609564, 4553048
        scale = 100 / (xmax - xmin)

        with HTTMock(response_content):
            landuse_geoms, water_geoms, roads_geoms = \
                preview.get_map_features(xmin, ymin, xmax, ymax, 2, scale, 'protomaps-XXXX')

        self.assertEqual(len(landuse_geoms), 90, 'Should have 90 landuse geometries')
        self.assertEqual(len(water_geoms), 1, 'Should have 1 water geometry')
        self.assertEqual(len(roads_geoms), 792, 'Should have 792 road geometries')

# coding=ascii

from __future__ import absolute_import, division, print_function

import os
import copy
import json
import csv
import re

import unittest
import tempfile
import shutil

from .. import SourceConfig

from ..conform import (
    GEOM_FIELDNAME,
    csv_source_to_csv, find_source_path, row_transform_and_convert,
    row_fxn_regexp, row_merge,
    row_extract_and_reproject, row_convert_to_out, row_fxn_join, row_fxn_format,
    row_fxn_prefixed_number, row_fxn_postfixed_street,
    row_fxn_postfixed_unit,
    row_fxn_remove_prefix, row_fxn_remove_postfix, row_fxn_chain,
    row_fxn_first_non_empty, row_fxn_constant,
    row_canonicalize_unit_and_number, conform_cli,
    convert_regexp_replace, normalize_ogr_filename_case,
    is_in, geojson_source_to_csv, check_source_tests
    )

" Return an x,y array given a wkt point string"
def wkt_pt(pt_str):
    pt = pt_str.strip().replace('POINT', '').replace('(', '').replace(')', '').strip().split(' ')
    return float(pt[0]), float(pt[1])

class TestConformTransforms (unittest.TestCase):
    "Test low level data transform functions"

    def test_row_convert_to_out(self):
        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": { "street": "s", "number": "n" }
                }]
            }
        }), "addresses", "default")

        r = row_convert_to_out(d, {
            "s": "MAPLE LN",
            "n": "123",
            GEOM_FIELDNAME: "POINT (-119.2 39.3)"
        })

        self.assertEqual({
            "type": "Feature",
            "properties": {
                "unit": "",
                "number": "123",
                "street": "MAPLE LN",
                "city": "",
                "region": "",
                "district": "",
                "postcode": "",
                "hash": "",
                "id": ""
            },
            "geometry": {
                "type": "Point",
                "coordinates": (-119.2, 39.3)
            }
        }, r)

    def test_row_merge(self):
        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": { "street": [ "n", "t" ] }
                }]
            }
        }), "addresses", "default")
        r = row_merge(d, {"n": "MAPLE", "t": "ST", "x": "foo"}, 'street')
        self.assertEqual({"oa:street": "MAPLE ST", "x": "foo", "t": "ST", "n": "MAPLE"}, r)

        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": { "city": [ "n", "t" ] }
                }]
            }
        }), "addresses", "default")
        r = row_merge(d, {"n": "Village of", "t": "Stanley", "x": "foo"}, 'city')
        self.assertEqual({"oa:city": "Village of Stanley", "x": "foo", "t": "Stanley", "n": "Village of"}, r)

    def test_row_fxn_join(self):
        "New fxn join"
        c = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": {
                            "function": "join",
                            "fields": ["a1"]
                        },
                        "street": {
                            "function": "join",
                            "fields": ["b1","b2"],
                            "separator": "-"
                        }
                    }
                }]
            }
        }), "addresses", "default")

        d = { "a1": "va1", "b1": "vb1", "b2": "vb2" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "va1", "oa:street": "vb1-vb2" })

        d = row_fxn_join(c, d, "number", c.data_source["conform"]["number"])
        d = row_fxn_join(c, d, "street", c.data_source["conform"]["street"])
        self.assertEqual(e, d)

        # ---

        d = { "a1": "va1", "b1": "vb1", "b2": None}
        e = copy.deepcopy(d)
        e.update({ "oa:number": "va1", "oa:street": "vb1" })

        d = row_fxn_join(c, d, "number", c.data_source["conform"]["number"])
        d = row_fxn_join(c, d, "street", c.data_source["conform"]["street"])

        self.assertEqual(e, d)

    def test_row_fxn_format(self):
        c = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": {
                            "function": "format",
                            "fields": ["a1", "a2", "a3"],
                            "format": "$1-$2-$3"
                        },
                        "street": {
                            "function": "format",
                            "fields": ["b1", "b2", "b3"],
                            "format": "foo $1$2-$3 bar"
                        }
                    }
                }]
            }
        }), "addresses", "default")

        d = {"a1": "12.0", "a2": "34", "a3": "56", "b1": "1", "b2": "B", "b3": "3"}
        e = copy.deepcopy(d)
        d = row_fxn_format(c, d, "number", c.data_source["conform"]["number"])
        d = row_fxn_format(c, d, "street", c.data_source["conform"]["street"])
        self.assertEqual(d.get("oa:number", ""), "12-34-56")
        self.assertEqual(d.get("oa:street", ""), "foo 1B-3 bar")

        d = copy.deepcopy(e)
        d["a2"] = None
        d["b3"] = None
        d = row_fxn_format(c, d, "number", c.data_source["conform"]["number"])
        d = row_fxn_format(c, d, "street", c.data_source["conform"]["street"])
        self.assertEqual(d.get("oa:number", ""), "12-56")
        self.assertEqual(d.get("oa:street", ""), "foo 1B bar")

    def test_row_fxn_chain(self):
        c = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": {
                            "function": "chain",
                            "functions": [
                                {
                                    "function": "format",
                                    "fields": ["a1", "a2", "a3"],
                                    "format": "$1-$2-$3"
                                },
                                {
                                    "function": "remove_postfix",
                                    "field": "oa:number",
                                    "field_to_remove": "b1"
                                }
                            ]
                        }
                    }
                }]
            }
        }), "addresses", "default")

        d = {"a1": "12", "a2": "34", "a3": "56 UNIT 5", "b1": "UNIT 5"}
        e = copy.deepcopy(d)
        d = row_fxn_chain(c, d, "number", c.data_source["conform"]["number"])
        self.assertEqual(d.get("oa:number", ""), "12-34-56")

        d = copy.deepcopy(e)
        d["a2"] = None
        d = row_fxn_chain(c, d, "number", c.data_source["conform"]["number"])
        self.assertEqual(d.get("oa:number", ""), "12-56")


    def test_row_fxn_chain_nested(self):
        c = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": {
                            "function": "chain",
                            "variable": "foo",
                            "functions": [{
                                "function": "format",
                                "fields": ["a1", "a2"],
                                "format": "$1-$2"
                            },{
                                "function": "chain",
                                "variable": "bar",
                                "functions": [{
                                    "function": "format",
                                    "fields": ["foo", "a3"],
                                    "format": "$1-$2"
                                },{
                                    "function": "remove_postfix",
                                    "field": "bar",
                                    "field_to_remove": "b1"
                                }]
                            }]
                        }
                    }
                }]
            }
        }), "addresses", "default")

        d = {"a1": "12", "a2": "34", "a3": "56 UNIT 5", "b1": "UNIT 5"}
        e = copy.deepcopy(d)
        d = row_fxn_chain(c, d, "number", c.data_source["conform"]["number"])
        self.assertEqual(d.get("oa:number", ""), "12-34-56")

        d = copy.deepcopy(e)
        d["a2"] = None
        d = row_fxn_chain(c, d, "number", c.data_source["conform"]["number"])
        self.assertEqual(d.get("oa:number", ""), "12-56")

    def test_row_fxn_regexp(self):
        "Regex split - replace"

        c = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": {
                            "function": "regexp",
                            "field": "ADDRESS",
                            "pattern": "^([0-9]+)(?:.*)",
                            "replace": "$1"
                        },
                        "street": {
                            "function": "regexp",
                            "field": "ADDRESS",
                            "pattern": "(?:[0-9]+ )(.*)",
                            "replace": "$1"
                        }
                    }
                }]
            }
        }), "addresses", "default")
        d = { "ADDRESS": "123 MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123", "oa:street": "MAPLE ST" })

        d = row_fxn_regexp(c, d, "number", c.data_source["conform"]["number"])
        d = row_fxn_regexp(c, d, "street", c.data_source["conform"]["street"])
        self.assertEqual(e, d)

        "Regex split - no replace - good match"
        c = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": {
                            "function": "regexp",
                            "field": "ADDRESS",
                            "pattern": "^([0-9]+)"
                        },
                        "street": {
                            "function": "regexp",
                            "field": "ADDRESS",
                            "pattern": "(?:[0-9]+ )(.*)"
                        }
                    }
                }]
            }
        }), "addresses", "default")
        d = { "ADDRESS": "123 MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123", "oa:street": "MAPLE ST" })

        d = row_fxn_regexp(c, d, "number", c.data_source["conform"]["number"])
        d = row_fxn_regexp(c, d, "street", c.data_source["conform"]["street"])
        self.assertEqual(e, d)

        "regex split - no replace - bad match"
        c = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": {
                            "function": "regexp",
                            "field": "ADDRESS",
                            "pattern": "^([0-9]+)"
                        },
                        "street": {
                            "function": "regexp",
                            "field": "ADDRESS",
                            "pattern": "(fake)"
                        }
                    }
                }]
            }
        }), "addresses", "default")
        d = { "ADDRESS": "123 MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123", "oa:street": "" })

        d = row_fxn_regexp(c, d, "number", c.data_source["conform"]["number"])
        d = row_fxn_regexp(c, d, "street", c.data_source["conform"]["street"])
        self.assertEqual(e, d)

    def test_transform_and_convert(self):
        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "street": ["s1", "s2"],
                        "number": "n",
                        "lon": "y",
                        "lat": "x"
                    },
                    "fingerprint": "0000"
                }]
            }
        }), "addresses", "default")

        r = row_transform_and_convert(d, { "n": "123", "s1": "MAPLE", "s2": "ST", "oa:geom": "POINT (-119.2 39.3)"})
        self.assertEqual({
            'type': 'Feature',
            'properties': {
                "street": "MAPLE ST",
                "unit": "",
                "number": "123",
                "city": "",
                "region": "",
                "district": "",
                "postcode": "",
                'hash': 'b3af08e447c7ed16',
                "id": ""
            },
            'geometry': {
                'coordinates': [-119.2, 39.3],
                'type': 'Point'
            }
        }, r)

        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": { "street": ["s1", "s2"], "number": "n", "lon": "y", "lat": "x" }, "fingerprint": "0000"
                }]
            }
        }), "addresses", "default")

        r = row_transform_and_convert(d, { "n": "123", "s1": "MAPLE", "s2": "ST", GEOM_FIELDNAME: "POINT(-119.2 39.3)"})
        self.assertEqual({
            'type': 'Feature',
            'properties': {
                "street": "MAPLE ST",
                "unit": "",
                "number": "123",
                "city": "",
                "region": "",
                "district": "",
                "postcode": "",
                "id": "",
                'hash': 'd4681f7e1d34e6ed'
            },
            'geometry': {
                "type": "Point",
                "coordinates": [-119.2, 39.3]
            }
        }, r)

        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": {
                            "function": "regexp",
                            "field": "s",
                            "pattern": "^(\\S+)"
                        },
                        "street": {
                            "function": "regexp",
                            "field": "s",
                            "pattern": "^(?:\\S+ )(.*)"
                        },
                        "lon": "y",
                        "lat": "x"
                    },
                    "fingerprint": "0000"
                }]
            }
        }), "addresses", "default")
        r = row_transform_and_convert(d, { "s": "123 MAPLE ST", GEOM_FIELDNAME: "POINT(-119.2 39.3)" })
        self.assertEqual({
            'type': 'Feature',
            'geometry': {
                "type": "Point",
                "coordinates": [-119.2, 39.3]
            },
            'properties': {
                "street": "MAPLE ST",
                "unit": "",
                "number": "123",
                "city": "",
                "region": "",
                "district": "",
                "postcode": "",
                "id": "",
                'hash': '591d7970b5753b0d'
            }
        }, r)

        "Test first_non_empty function with nothing found"
        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "number": {
                            "function": "first_non_empty",
                            "fields": ["a", "b"]
                        },
                        "lon": "y",
                        "lat": "x",
                    },
                    "fingerprint": "0000"
                }]
            }
        }), "addresses", "default")
        r = row_transform_and_convert(d, { "a": "", "b": "", GEOM_FIELDNAME: "POINT(-119.2 39.3)" })
        self.assertEqual({
            'type': 'Feature',
            'geometry': {
                "type": "Point",
                "coordinates": [-119.2, 39.3]
            },
            'properties': {
                "street": "",
                "unit": "",
                "number": "",
                "city": "",
                "region": "",
                "district": "",
                "postcode": "",
                "id": "",
                'hash': '7b1dc0b74cbc0162'
            }
        }, r)

    def test_row_canonicalize_unit_and_number(self):
        r = row_canonicalize_unit_and_number({}, {"number": "324 ", "street": " OAK DR.", "unit": "1"})
        self.assertEqual("324", r["number"])
        self.assertEqual("OAK DR.", r["street"])
        self.assertEqual("1", r["unit"])

        # Tests for integer conversion
        for e, a in (("324", " 324.0  "),
                     ("", ""),
                     ("3240", "3240"),
                     ("INVALID", "INVALID"),
                     ("324.5", "324.5")):
            r = row_canonicalize_unit_and_number({}, {"number": a, "street": "", "unit": ""})
            self.assertEqual(e, r["number"])

    def test_row_canonicalize_street_and_no_number(self):
        r = row_canonicalize_unit_and_number({}, {"number": None, "street": " OAK DR.", "unit": None})
        self.assertEqual("", r["number"])
        self.assertEqual("OAK DR.", r["street"])
        self.assertEqual("", r["unit"])

    def test_row_canonicalize_street_with_no_unit_number(self):
        r = row_canonicalize_unit_and_number({}, {"number": None, "street": " OAK DR.", "unit": None})
        self.assertEqual("", r["number"])
        self.assertEqual("OAK DR.", r["street"])
        self.assertEqual("", r["unit"])

    def test_row_extract_and_reproject(self):
        # CSV lat/lon column names
        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "lon": "longitude",
                        "lat": "latitude",
                        "format": "csv"
                    },
                    'protocol': 'test'
                }]
            }
        }), "addresses", "default")
        r = row_extract_and_reproject(d, {"longitude": "-122.3", "latitude": "39.1"})
        self.assertEqual({GEOM_FIELDNAME: "POINT (-122.3 39.1)"}, r)

        # non-CSV lat/lon column names
        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "lon": "x",
                        "lat": "y",
                        "format": ""
                    },
                    'protocol': 'test'
                }]
            }
        }), "addresses", "default")
        r = row_extract_and_reproject(d, {"oa:geom": "POINT (-122.3 39.1)"})
        self.assertEqual({GEOM_FIELDNAME: "POINT (-122.3 39.1)"}, r)

        # reprojection
        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform" : {
                        "srs": "EPSG:2913",
                        "format": ""
                    },
                    'protocol': 'test'
                }]
            }
        }), "addresses", "default")
        r = row_extract_and_reproject(d, {GEOM_FIELDNAME: "POINT (7655634.924 668868.414)"})

        x,y = wkt_pt(r[GEOM_FIELDNAME])
        self.assertAlmostEqual(-122.630842186651, x, places=4)
        self.assertAlmostEqual(45.4815543938511, y, places=4)

        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform" : {
                        "lon": "X",
                        "lat": "Y",
                        "srs": "EPSG:2913",
                        "format": "csv"
                    },
                    'protocol': 'test'
                }]
            }
        }), "addresses", "default")
        r = row_extract_and_reproject(d, {"X": "", "Y": ""})
        self.assertEqual(None, r[GEOM_FIELDNAME])

        # commas in lat/lon columns (eg Iceland)
        d = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform" : {
                        "lon": "LONG_WGS84",
                        "lat": "LAT_WGS84",
                        "format": "csv"
                    },
                    'protocol': 'test'
                }]
            }
        }), "addresses", "default")
        r = row_extract_and_reproject(d, {"LONG_WGS84": "-21,77", "LAT_WGS84": "64,11"})
        self.assertEqual({GEOM_FIELDNAME: "POINT (-21.77 64.11)"}, r)

    def test_row_fxn_prefixed_number_and_postfixed_street_no_units(self):
        "Regex prefixed_number and postfix_street - both fields present"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123", "oa:street": "MAPLE ST" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - no number"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "", "oa:street": "MAPLE ST" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - empty input"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "", "oa:street": "" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - no spaces after number"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "", "oa:street": "123MAPLE ST" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - excess whitespace"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": " \t 123 \t MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123", "oa:street": "MAPLE ST" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_number - ordinal street w/house number"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "12 3RD ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "12", "oa:street": "3RD ST" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_number - ordinal street w/o house number"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "3RD ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "", "oa:street": "3RD ST" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_number - combined house number and suffix"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123A 3RD ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123A", "oa:street": "3RD ST" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_number - hyphenated house number and suffix"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123-A 3RD ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123-A", "oa:street": "3RD ST" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_number - queens-style house number"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123-45 3RD ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123-45", "oa:street": "3RD ST" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_number - should be case-insenstive"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123-a 3rD St" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123-a", "oa:street": "3rD St" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - should honor space+1/2"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123 1/2 3rD St" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123 1/2", "oa:street": "3rD St" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - should honor hyphen+1/2"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123-1/2 3rD St" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123-1/2", "oa:street": "3rD St" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - should honor space+1/3"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123 1/3 3rD St" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123 1/3", "oa:street": "3rD St" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - should honor hyphen+1/3"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123-1/3 3rD St" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123-1/3", "oa:street": "3rD St" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - should honor space+1/4"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123 1/4 3rD St" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123 1/4", "oa:street": "3rD St" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - should honor hyphen+1/4"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123-1/4 3rD St" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123-1/4", "oa:street": "3rD St" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - should honor space+3/4"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123 3/4 3rD St" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123 3/4", "oa:street": "3rD St" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "Regex prefixed_number and postfixed_street - should honor hyphen+3/4"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123-3/4 3rD St" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123-3/4", "oa:street": "3rD St" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "contains unit but may_contain_units is not present"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST UNIT 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123", "oa:street": "MAPLE ST UNIT 3" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "contains unit but may_contain_units is explicitly false"
        c = { "conform": {
            "number": {
                "function": "prefixed_number",
                "field": "ADDRESS"
            },
            "street": {
                "function": "postfixed_street",
                "may_contain_units": False,
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST UNIT 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:number": "123", "oa:street": "MAPLE ST UNIT 3" })

        d = row_fxn_prefixed_number(c, d, "number", c["conform"]["number"])
        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

    def test_row_fxn_prefixed_number_and_postfixed_street_may_contain_units(self):
        "UNIT-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST UNIT 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "APARTMENT-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST APARTMENT 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "APT-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST APT 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "APT.-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST APT. 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "SUITE-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST SUITE 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "STE-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST STE 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "STE.-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST STE. 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "BUILDING-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST BUILDING 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "BLDG-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST BLDG 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "BLDG.-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST BLDG. 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "LOT-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST LOT 3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "#-style unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST #3" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "no unit"
        c = { "conform": {
            "street": {
                "function": "postfixed_street",
                "field": "ADDRESS",
                "may_contain_units": True
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_postfixed_street(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

    def test_row_fxn_postfixed_unit(self):
        "postfixed_unit - UNIT-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Unit 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Unit 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - UNIT is word ending"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street runit 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - APARTMENT-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Apartment 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Apartment 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - APT-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Apt 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Apt 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - APT is word ending"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street rapt 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - APT.-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Apt. 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Apt. 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - SUITE-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Suite 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Suite 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - STE-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Ste 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Ste 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - STE is word ending"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Haste 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - STE.-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Ste. 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Ste. 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - BUILDING-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Building 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Building 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - BLDG-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Bldg 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Bldg 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - BLDG.-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Bldg. 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Bldg. 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - LOT-style"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street Lot 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "Lot 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - LOT is word ending"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street alot 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - #-style with spaces"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street # 300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "# 300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - #-style without spaces"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street #300" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "#300" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

        "postfixed_unit - no unit"
        c = { "conform": {
            "unit": {
                "function": "postfixed_unit",
                "field": "ADDRESS"
            }
        } }
        d = { "ADDRESS": "Main Street" }
        e = copy.deepcopy(d)
        e.update({ "oa:unit": "" })

        d = row_fxn_postfixed_unit(c, d, "unit", c["conform"]["unit"])
        self.assertEqual(e, d)

    def test_row_fxn_remove_prefix(self):
        "remove_prefix - field_to_remove is a prefix"
        c = { "conform": {
            "street": {
                "function": "remove_prefix",
                "field": "ADDRESS",
                "field_to_remove": "PREFIX"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST", "PREFIX": "123" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_remove_prefix(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "remove_prefix - field_to_remove is not a prefix"
        c = { "conform": {
            "street": {
                "function": "remove_prefix",
                "field": "ADDRESS",
                "field_to_remove": "PREFIX"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST", "PREFIX": "NOT THE PREFIX VALUE" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "123 MAPLE ST" })

        d = row_fxn_remove_prefix(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "remove_prefix - field_to_remove value is empty string"
        c = { "conform": {
            "street": {
                "function": "remove_prefix",
                "field": "ADDRESS",
                "field_to_remove": "PREFIX"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST", "PREFIX": "" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "123 MAPLE ST" })

        d = row_fxn_remove_prefix(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

    def test_row_fxn_remove_postfix(self):
        "remove_postfix - field_to_remove is a postfix"
        c = { "conform": {
            "street": {
                "function": "remove_postfix",
                "field": "ADDRESS",
                "field_to_remove": "POSTFIX"
            }
        } }
        d = { "ADDRESS": "MAPLE ST UNIT 5", "POSTFIX": "UNIT 5" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "MAPLE ST" })

        d = row_fxn_remove_postfix(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "remove_postfix - field_to_remove is not a postfix"
        c = { "conform": {
            "street": {
                "function": "remove_postfix",
                "field": "ADDRESS",
                "field_to_remove": "POSTFIX"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST", "POSTFIX": "NOT THE POSTFIX VALUE" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "123 MAPLE ST" })

        d = row_fxn_remove_postfix(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "remove_postfix - field_to_remove value is empty string"
        c = { "conform": {
            "street": {
                "function": "remove_postfix",
                "field": "ADDRESS",
                "field_to_remove": "POSTFIX"
            }
        } }
        d = { "ADDRESS": "123 MAPLE ST", "POSTFIX": "" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "123 MAPLE ST" })

        d = row_fxn_remove_postfix(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

    def test_row_first_non_empty(self):
        "first_non_empty - fields array is empty"
        c = { "conform": {
            "street": {
                "function": "first_non_empty",
                "fields": []
            }
        } }
        d = { }
        e = copy.deepcopy(d)
        e.update({ })

        d = row_fxn_first_non_empty(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "first_non_empty - both fields are non-empty"
        c = { "conform": {
            "street": {
                "function": "first_non_empty",
                "fields": ["FIELD1", "FIELD2"]
            }
        } }
        d = { "FIELD1": "field1 value", "FIELD2": "field2 value" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "field1 value" })

        d = row_fxn_first_non_empty(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "first_non_empty - first field is null"
        c = { "conform": {
            "street": {
                "function": "first_non_empty",
                "fields": ["FIELD1", "FIELD2"]
            }
        } }
        d = { "FIELD1": None, "FIELD2": "field2 value" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "field2 value" })

        d = row_fxn_first_non_empty(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "first_non_empty - first field is 0-length string"
        c = { "conform": {
            "street": {
                "function": "first_non_empty",
                "fields": ["FIELD1", "FIELD2"]
            }
        } }
        d = { "FIELD1": "", "FIELD2": "field2 value" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "field2 value" })

        d = row_fxn_first_non_empty(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "first_non_empty - first field is trimmable to a 0-length string"
        c = { "conform": {
            "street": {
                "function": "first_non_empty",
                "fields": ["FIELD1", "FIELD2"]
            }
        } }
        d = { "FIELD1": " \t ", "FIELD2": "field2 value" }
        e = copy.deepcopy(d)
        e.update({ "oa:street": "field2 value" })

        d = row_fxn_first_non_empty(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

        "first_non_empty - all field values are trimmable to a 0-length string"
        c = { "conform": {
            "street": {
                "function": "first_non_empty",
                "fields": ["FIELD1", "FIELD2"]
            }
        } }
        d = { "FIELD1": " \t ", "FIELD2": " \t " }
        e = copy.deepcopy(d)
        e.update({ })

        d = row_fxn_first_non_empty(c, d, "street", c["conform"]["street"])
        self.assertEqual(e, d)

    def test_row_fxn_constant(self):
        "constant - replacing empty string"
        c = { "conform": {
            "region": {
                "function": "constant",
                "value": "PA"
            }
        } }
        d = { "STATE": "" }
        e = copy.deepcopy(d)
        e.update({ "oa:region": "PA" })

        d = row_fxn_constant(c, d, "region", c["conform"]["region"])
        self.assertEqual(e, d)

        "constant - replacing non-empty, non-standard string"
        c = { "conform": {
            "region": {
                "function": "constant",
                "value": "PA"
            }
        } }
        d = { "STATE": "Penna" }
        e = copy.deepcopy(d)
        e.update({ "oa:region": "PA" })

        d = row_fxn_constant(c, d, "region", c["conform"]["region"])
        self.assertEqual(e, d)

class TestConformCli (unittest.TestCase):
    "Test the command line interface creates valid output files from test input"
    def setUp(self):
        self.testdir = tempfile.mkdtemp(prefix='openaddr-testPyConformCli-')
        self.conforms_dir = os.path.join(os.path.dirname(__file__), 'conforms')

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def _run_conform_on_source(self, source_name, ext):
        "Helper method to run a conform on the named source. Assumes naming convention."
        with open(os.path.join(self.conforms_dir, "%s.json" % source_name)) as file:
            source_config = SourceConfig(json.load(file), "addresses", "default")
        source_path = os.path.join(self.conforms_dir, "%s.%s" % (source_name, ext))
        dest_path = os.path.join(self.testdir, '%s-conformed.csv' % source_name)

        rc = conform_cli(source_config, source_path, dest_path)
        return rc, dest_path

    def test_unknown_conform(self):
        # Test that the conform tool does something reasonable with unknown conform sources
        self.assertEqual(1, conform_cli(SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default"
                }]
            }
        }), "addresses", "default"), 'test', ''))
        self.assertEqual(1, conform_cli(SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {}
                }]
            }
        }), "addresses", "default"), 'test', ''))
        self.assertEqual(1, conform_cli(SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": {
                        "format": "broken"
                    }
                }]
            }
        }), "addresses", "default"), 'test', ''))

    def test_lake_man(self):
        rc, dest_path = self._run_conform_on_source('lake-man', 'shp')
        self.assertEqual(0, rc)

        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))

            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.2592497, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.8026126, rows[0]['geometry']['coordinates'][1], places=4)

            self.assertEqual(6, len(rows))
            self.assertEqual(rows[0]['properties']['number'], '5115')
            self.assertEqual(rows[0]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[1]['properties']['number'], '5121')
            self.assertEqual(rows[1]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[2]['properties']['number'], '5133')
            self.assertEqual(rows[2]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[3]['properties']['number'], '5126')
            self.assertEqual(rows[3]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[4]['properties']['number'], '5120')
            self.assertEqual(rows[4]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[5]['properties']['number'], '5115')
            self.assertEqual(rows[5]['properties']['street'], 'OLD MILL RD')

    def test_lake_man_gdb(self):
        rc, dest_path = self._run_conform_on_source('lake-man-gdb', 'gdb')
        self.assertEqual(0, rc)

        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))

            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.2592497, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.8026126, rows[0]['geometry']['coordinates'][1], places=4)

            self.assertEqual(6, len(rows))
            self.assertEqual(rows[0]['properties']['number'], '5115')
            self.assertEqual(rows[0]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[1]['properties']['number'], '5121')
            self.assertEqual(rows[1]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[2]['properties']['number'], '5133')
            self.assertEqual(rows[2]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[3]['properties']['number'], '5126')
            self.assertEqual(rows[3]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[4]['properties']['number'], '5120')
            self.assertEqual(rows[4]['properties']['street'], 'FRUITED PLAINS LN')
            self.assertEqual(rows[5]['properties']['number'], '5115')
            self.assertEqual(rows[5]['properties']['street'], 'OLD MILL RD')

    def test_lake_man_split(self):
        rc, dest_path = self._run_conform_on_source('lake-man-split', 'shp')
        self.assertEqual(0, rc)

        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))

            self.assertEqual(rows[0]['properties']['number'], '915')
            self.assertEqual(rows[0]['properties']['street'], 'EDWARD AVE')
            self.assertEqual(rows[1]['properties']['number'], '3273')
            self.assertEqual(rows[1]['properties']['street'], 'PETER ST')
            self.assertEqual(rows[2]['properties']['number'], '976')
            self.assertEqual(rows[2]['properties']['street'], 'FORD BLVD')
            self.assertEqual(rows[3]['properties']['number'], '7055')
            self.assertEqual(rows[3]['properties']['street'], 'ST ROSE AVE')
            self.assertEqual(rows[4]['properties']['number'], '534')
            self.assertEqual(rows[4]['properties']['street'], 'WALLACE AVE')
            self.assertEqual(rows[5]['properties']['number'], '531')
            self.assertEqual(rows[5]['properties']['street'], 'SCOFIELD AVE')

    def test_lake_man_merge_postcode(self):
        rc, dest_path = self._run_conform_on_source('lake-man-merge-postcode', 'shp')
        self.assertEqual(0, rc)

        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))

            self.assertEqual(rows[0]['properties']['number'], '35845')
            self.assertEqual(rows[0]['properties']['street'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[1]['properties']['number'], '35850')
            self.assertEqual(rows[1]['properties']['street'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[2]['properties']['number'], '35900')
            self.assertEqual(rows[2]['properties']['street'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[3]['properties']['number'], '35870')
            self.assertEqual(rows[3]['properties']['street'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[4]['properties']['number'], '32551')
            self.assertEqual(rows[4]['properties']['street'], 'EKLUTNA LAKE RD')
            self.assertEqual(rows[5]['properties']['number'], '31401')
            self.assertEqual(rows[5]['properties']['street'], 'EKLUTNA LAKE RD')

    def test_lake_man_merge_postcode2(self):
        rc, dest_path = self._run_conform_on_source('lake-man-merge-postcode2', 'shp')
        self.assertEqual(0, rc)

        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))

            self.assertEqual(rows[0]['properties']['number'], '85')
            self.assertEqual(rows[0]['properties']['street'], 'MAITLAND DR')
            self.assertEqual(rows[1]['properties']['number'], '81')
            self.assertEqual(rows[1]['properties']['street'], 'MAITLAND DR')
            self.assertEqual(rows[2]['properties']['number'], '92')
            self.assertEqual(rows[2]['properties']['street'], 'MAITLAND DR')
            self.assertEqual(rows[3]['properties']['number'], '92')
            self.assertEqual(rows[3]['properties']['street'], 'MAITLAND DR')
            self.assertEqual(rows[4]['properties']['number'], '92')
            self.assertEqual(rows[4]['properties']['street'], 'MAITLAND DR')
            self.assertEqual(rows[5]['properties']['number'], '92')
            self.assertEqual(rows[5]['properties']['street'], 'MAITLAND DR')

    def test_lake_man_shp_utf8(self):
        rc, dest_path = self._run_conform_on_source('lake-man-utf8', 'shp')
        self.assertEqual(0, rc)
        with open(dest_path, encoding='utf-8') as fp:
            rows = list(map(json.loads, list(fp)))

            self.assertEqual(rows[0]['properties']['street'], u'PZ ESPA\u00d1A')

    def test_lake_man_shp_epsg26943(self):
        rc, dest_path = self._run_conform_on_source('lake-man-epsg26943', 'shp')
        self.assertEqual(0, rc)

        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))
            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.2592497, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.8026126, rows[0]['geometry']['coordinates'][1], places=4)

    def test_lake_man_shp_noprj_epsg26943(self):
        rc, dest_path = self._run_conform_on_source('lake-man-epsg26943-noprj', 'shp')
        self.assertEqual(0, rc)

        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))
            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.2592497, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.8026126, rows[0]['geometry']['coordinates'][1], places=4)

    # TODO: add tests for non-ESRI GeoJSON sources

    def test_lake_man_split2(self):
        "An ESRI-to-CSV like source"
        rc, dest_path = self._run_conform_on_source('lake-man-split2', 'csv')
        self.assertEqual(0, rc)

        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))
            self.assertEqual(rows[0]['properties']['number'], '1')
            self.assertEqual(rows[0]['properties']['street'], 'Spectrum Pointe Dr #320')
            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.25925, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.802613, rows[0]['geometry']['coordinates'][1], places=4)

            self.assertEqual(rows[1]['properties']['number'], '')
            self.assertEqual(rows[1]['properties']['street'], '')
            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.25925, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.802613, rows[0]['geometry']['coordinates'][1], places=4)

            self.assertEqual(rows[2]['properties']['number'], '300')
            self.assertEqual(rows[2]['properties']['street'], 'E Chapman Ave')
            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.25925, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.802613, rows[0]['geometry']['coordinates'][1], places=4)

            self.assertEqual(rows[3]['properties']['number'], '1')
            self.assertEqual(rows[3]['properties']['street'], 'Spectrum Pointe Dr #320')
            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.25925, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.802613, rows[0]['geometry']['coordinates'][1], places=4)

            self.assertEqual(rows[4]['properties']['number'], '1')
            self.assertEqual(rows[4]['properties']['street'], 'Spectrum Pointe Dr #320')
            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.25925, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.802613, rows[0]['geometry']['coordinates'][1], places=4)

            self.assertEqual(rows[5]['properties']['number'], '1')
            self.assertEqual(rows[5]['properties']['street'], 'Spectrum Pointe Dr #320')
            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(-122.25925, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.802613, rows[0]['geometry']['coordinates'][1], places=4)

    def test_nara_jp(self):
        "Test case from jp-nara.json"
        rc, dest_path = self._run_conform_on_source('jp-nara', 'csv')
        self.assertEqual(0, rc)
        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))
            self.assertEqual(rows[0]['properties']['number'], '2543-6')
            self.assertEqual('Point', rows[0]['geometry']['type'])
            self.assertAlmostEqual(135.955104, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(34.607832, rows[0]['geometry']['coordinates'][1], places=4)
            self.assertEqual(rows[0]['properties']['street'], u'\u91dd\u753a')
            self.assertEqual(rows[1]['properties']['number'], '202-6')

    def test_lake_man_3740(self):
        "CSV in an oddball SRS"
        rc, dest_path = self._run_conform_on_source('lake-man-3740', 'csv')
        self.assertEqual(0, rc)
        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))

            # POINT (-122.2592495 37.8026123)
            self.assertAlmostEqual(-122.2592495, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(37.8026123, rows[0]['geometry']['coordinates'][1], places=4)

            self.assertEqual(rows[0]['properties']['number'], '5')
            self.assertEqual(rows[0]['properties']['street'], u'PZ ESPA\u00d1A')

    def test_lake_man_gml(self):
        "GML XML files"
        rc, dest_path = self._run_conform_on_source('lake-man-gml', 'gml')
        self.assertEqual(0, rc)
        with open(dest_path) as fp:
            rows = list(map(json.loads, list(fp)))
            self.assertEqual(6, len(rows))
            self.assertAlmostEqual(37.8026126, rows[0]['geometry']['coordinates'][0], places=4)
            self.assertAlmostEqual(-122.2592497, rows[0]['geometry']['coordinates'][1], places=4)
            self.assertEqual(rows[0]['properties']['number'], '5115')
            self.assertEqual(rows[0]['properties']['street'], 'FRUITED PLAINS LN')


class TestConformMisc(unittest.TestCase):

    def setUp(self):
        self.testdir = tempfile.mkdtemp(prefix='openaddr-TestConformMisc-')

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def test_convert_regexp_replace(self):
        '''
        '''
        crr = convert_regexp_replace

        self.assertEqual(crr('$1'), r'\1')
        self.assertEqual(crr('$9'), r'\9')
        self.assertEqual(crr('$b'), '$b')
        self.assertEqual(crr('$1yo$1'), r'\1yo\1')
        self.assertEqual(crr('$9yo$9'), r'\9yo\9')
        self.assertEqual(crr('$byo$b'), '$byo$b')
        self.assertEqual(crr('$1 yo $1'), r'\1 yo \1')
        self.assertEqual(crr('$9 yo $9'), r'\9 yo \9')
        self.assertEqual(crr('$b yo $b'), '$b yo $b')

        self.assertEqual(crr('$11'), r'\11')
        self.assertEqual(crr('$99'), r'\99')
        self.assertEqual(crr('$bb'), '$bb')
        self.assertEqual(crr('$11yo$11'), r'\11yo\11')
        self.assertEqual(crr('$99yo$99'), r'\99yo\99')
        self.assertEqual(crr('$bbyo$bb'), '$bbyo$bb')
        self.assertEqual(crr('$11 yo $11'), r'\11 yo \11')
        self.assertEqual(crr('$99 yo $99'), r'\99 yo \99')
        self.assertEqual(crr('$bb yo $bb'), '$bb yo $bb')

        self.assertEqual(crr('${1}1'), r'\g<1>1')
        self.assertEqual(crr('${9}9'), r'\g<9>9')
        self.assertEqual(crr('${9}b'), r'\g<9>b')
        self.assertEqual(crr('${b}b'), '${b}b')
        self.assertEqual(crr('${1}1yo${1}1'), r'\g<1>1yo\g<1>1')
        self.assertEqual(crr('${9}9yo${9}9'), r'\g<9>9yo\g<9>9')
        self.assertEqual(crr('${9}byo${9}b'), r'\g<9>byo\g<9>b')
        self.assertEqual(crr('${b}byo${b}b'), '${b}byo${b}b')
        self.assertEqual(crr('${1}1 yo ${1}1'), r'\g<1>1 yo \g<1>1')
        self.assertEqual(crr('${9}9 yo ${9}9'), r'\g<9>9 yo \g<9>9')
        self.assertEqual(crr('${9}b yo ${9}b'), r'\g<9>b yo \g<9>b')
        self.assertEqual(crr('${b}b yo ${b}b'), '${b}b yo ${b}b')

        self.assertEqual(crr('${11}1'), r'\g<11>1')
        self.assertEqual(crr('${99}9'), r'\g<99>9')
        self.assertEqual(crr('${99}b'), r'\g<99>b')
        self.assertEqual(crr('${bb}b'), '${bb}b')
        self.assertEqual(crr('${11}1yo${11}1'), r'\g<11>1yo\g<11>1')
        self.assertEqual(crr('${99}9yo${99}9'), r'\g<99>9yo\g<99>9')
        self.assertEqual(crr('${99}byo${99}b'), r'\g<99>byo\g<99>b')
        self.assertEqual(crr('${bb}byo${bb}b'), '${bb}byo${bb}b')
        self.assertEqual(crr('${11}1yo${11}1'), r'\g<11>1yo\g<11>1')
        self.assertEqual(crr('${99}9 yo ${99}9'), r'\g<99>9 yo \g<99>9')
        self.assertEqual(crr('${99}b yo ${99}b'), r'\g<99>b yo \g<99>b')
        self.assertEqual(crr('${bb}b yo ${bb}b'), '${bb}b yo ${bb}b')

        self.assertEqual(re.sub(r'hello (world)', crr('goodbye $1'), 'hello world'), 'goodbye world')
        self.assertEqual(re.sub(r'(hello) (world)', crr('goodbye $2'), 'hello world'), 'goodbye world')
        self.assertEqual(re.sub(r'he(ll)o', crr('he$1$1o'), 'hello'), 'hellllo')

    def test_find_shapefile_source_path(self):
        shp_conform = {"conform": { "format": "shapefile" } }
        self.assertEqual("foo.shp", find_source_path(shp_conform, ["foo.shp"]))
        self.assertEqual("FOO.SHP", find_source_path(shp_conform, ["FOO.SHP"]))
        self.assertEqual("xyzzy/FOO.SHP", find_source_path(shp_conform, ["xyzzy/FOO.SHP"]))
        self.assertEqual("foo.shp", find_source_path(shp_conform, ["foo.shp", "foo.prj", "foo.shx"]))
        self.assertEqual(None, find_source_path(shp_conform, ["nope.txt"]))
        self.assertEqual(None, find_source_path(shp_conform, ["foo.shp", "bar.shp"]))

        shp_file_conform = {"conform": { "format": "shapefile", "file": "foo.shp" } }
        self.assertEqual("foo.shp", find_source_path(shp_file_conform, ["foo.shp"]))
        self.assertEqual("foo.shp", find_source_path(shp_file_conform, ["foo.shp", "bar.shp"]))
        self.assertEqual("xyzzy/foo.shp", find_source_path(shp_file_conform, ["xyzzy/foo.shp", "xyzzy/bar.shp"]))

        shp_poly_conform = {"conform": { "format": "shapefile" } }
        self.assertEqual("foo.shp", find_source_path(shp_poly_conform, ["foo.shp"]))

        broken_conform = {"conform": { "format": "broken" }}
        self.assertEqual(None, find_source_path(broken_conform, ["foo.shp"]))

    def test_find_gdb_source_path(self):
        shp_conform = {"conform": { "format": "gdb" } }
        self.assertEqual("foo.gdb", find_source_path(shp_conform, ["foo.gdb"]))
        self.assertEqual("FOO.GDB", find_source_path(shp_conform, ["FOO.GDB"]))
        self.assertEqual("xyzzy/FOO.GDB", find_source_path(shp_conform, ["xyzzy/FOO.GDB"]))
        self.assertEqual("foo.gdb", find_source_path(shp_conform, ["foo.gdb", "foo.prj", "foo.shx"]))
        self.assertEqual(None, find_source_path(shp_conform, ["nope.txt"]))
        self.assertEqual(None, find_source_path(shp_conform, ["foo.gdb", "bar.gdb"]))

        shp_file_conform = {"conform": { "format": "gdb", "file": "foo.gdb" } }
        self.assertEqual("foo.gdb", find_source_path(shp_file_conform, ["foo.gdb"]))
        self.assertEqual("foo.gdb", find_source_path(shp_file_conform, ["foo.gdb", "bar.gdb"]))
        self.assertEqual("xyzzy/foo.gdb", find_source_path(shp_file_conform, ["xyzzy/foo.gdb", "xyzzy/bar.gdb"]))

    def test_find_geojson_source_path(self):
        geojson_conform = {"protocol": "notESRI", "conform": {"format": "geojson"}}
        self.assertEqual("foo.json", find_source_path(geojson_conform, ["foo.json"]))
        self.assertEqual("FOO.JSON", find_source_path(geojson_conform, ["FOO.JSON"]))
        self.assertEqual("xyzzy/FOO.JSON", find_source_path(geojson_conform, ["xyzzy/FOO.JSON"]))
        self.assertEqual("foo.json", find_source_path(geojson_conform, ["foo.json", "foo.prj", "foo.shx"]))
        self.assertEqual(None, find_source_path(geojson_conform, ["nope.txt"]))
        self.assertEqual(None, find_source_path(geojson_conform, ["foo.json", "bar.json"]))

    def test_find_esri_source_path(self):
        # test that the legacy ESRI/GeoJSON style works
        old_conform = {"protocol": "ESRI", "conform": {"format": "geojson"}}
        self.assertEqual("foo.csv", find_source_path(old_conform, ["foo.csv"]))
        # test that the new ESRI/CSV style works
        new_conform = {"protocol": "ESRI", "conform": {"format": "csv"}}
        self.assertEqual("foo.csv", find_source_path(new_conform, ["foo.csv"]))

    def test_find_csv_source_path(self):
        csv_conform = {"conform": {"format": "csv"}}
        self.assertEqual("foo.csv", find_source_path(csv_conform, ["foo.csv"]))
        csv_file_conform = {"conform": {"format": "csv", "file":"bar.txt"}}
        self.assertEqual("bar.txt", find_source_path(csv_file_conform, ["license.pdf", "bar.txt"]))
        self.assertEqual("aa/bar.txt", find_source_path(csv_file_conform, ["license.pdf", "aa/bar.txt"]))
        self.assertEqual(None, find_source_path(csv_file_conform, ["foo.txt"]))

    def test_find_xml_source_path(self):
        c = {"conform": {"format": "xml"}}
        self.assertEqual("foo.gml", find_source_path(c, ["foo.gml"]))
        c = {"conform": {"format": "xml", "file": "xyzzy/foo.gml"}}
        self.assertEqual("xyzzy/foo.gml", find_source_path(c, ["xyzzy/foo.gml", "bar.gml", "foo.gml"]))
        self.assertEqual("/tmp/foo/xyzzy/foo.gml", find_source_path(c, ["/tmp/foo/xyzzy/foo.gml"]))

    def test_normalize_ogr_filename_case1(self):
        filename = os.path.join(self.testdir, 'file.geojson')
        with open(filename, 'w') as file:
            file.write('yo')

        self.assertEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))

    def test_normalize_ogr_filename_case2(self):
        filename = os.path.join(self.testdir, 'file.GeoJSON')
        with open(filename, 'w') as file:
            file.write('yo')

        self.assertNotEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))

    def test_normalize_ogr_filename_case3(self):
        filename = os.path.join(self.testdir, 'file.shp')
        with open(filename, 'w') as file:
            file.write('yo')

        for otherbase in ('file.shx', 'file.dbf', 'file.prj'):
            othername = os.path.join(self.testdir, otherbase)
            with open(othername, 'w') as other:
                other.write('yo')

        self.assertEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.shx')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.dbf')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.prj')))

    def test_normalize_ogr_filename_case4(self):
        filename = os.path.join(self.testdir, 'file.Shp')
        with open(filename, 'w') as file:
            file.write('yo')

        for otherbase in ('file.Shx', 'file.Dbf', 'file.Prj'):
            othername = os.path.join(self.testdir, otherbase)
            with open(othername, 'w') as other:
                other.write('yo')

        self.assertNotEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.shx')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.dbf')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.prj')))

    def test_normalize_ogr_filename_case5(self):
        filename = os.path.join(self.testdir, 'file.SHP')
        with open(filename, 'w') as file:
            file.write('yo')

        for otherbase in ('file.SHX', 'file.DBF', 'file.PRJ'):
            othername = os.path.join(self.testdir, otherbase)
            with open(othername, 'w') as other:
                other.write('yo')

        self.assertNotEqual(normalize_ogr_filename_case(filename), filename)
        self.assertTrue(os.path.exists(normalize_ogr_filename_case(filename)))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.shx')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.dbf')))
        self.assertTrue(os.path.exists(os.path.join(self.testdir, 'file.prj')))

    def test_is_not_in(self):
        self.assertFalse(is_in('foo', []), 'Should not match an empty list')
        self.assertFalse(is_in('foo', ['bar']), 'Should not match')
        self.assertTrue(is_in('foo', ['foo']), 'Should be a simple match')
        self.assertTrue(is_in('Foo', ['foo']), 'Should be a case-insensitive match')

        self.assertFalse(is_in('foo/bar', ['bar']), 'Should not match in a directory')
        self.assertTrue(is_in('foo/bar', ['foo']), 'Should match a directory name')
        self.assertTrue(is_in('Foo/bar', ['foo']), 'Should match a directory case-insensitively')

        self.assertFalse(is_in('foo/bar/baz', ['baz']), 'Should not match in a nested directory')
        self.assertTrue(is_in('foo/bar', ['foo/bar']), 'Should match a directory path')
        self.assertTrue(is_in('foo/bar/baz', ['foo/bar']), 'Should match a directory path')
        self.assertTrue(is_in('foo/bar/baz', ['foo']), 'Should match a directory path')
        self.assertTrue(is_in('Foo/bar/baz', ['foo']), 'Should match a directory path case-insensitively')
        self.assertTrue(is_in('foo/Bar', ['foo/bar']), 'Should match a directory path case-insensitively')
        self.assertTrue(is_in('foo/Bar/baz', ['foo/bar']), 'Should match a directory path case-insensitively')

    def test_geojson_source_to_csv(self):
        '''
        '''
        c = SourceConfig(dict({
            "schema": 2,
            "layers": {
                "addresses": [{
                    "name": "default",
                    "conform": { }
                }]
            }
        }), "addresses", "default")

        geojson_path = os.path.join(os.path.dirname(__file__), 'data/us-pa-bucks.geojson')
        csv_path = os.path.join(self.testdir, 'us-tx-waco.csv')
        geojson_source_to_csv(c, geojson_path, csv_path)

        with open(csv_path, encoding='utf8') as file:
            row = next(csv.DictReader(file))
            self.assertEqual(row[GEOM_FIELDNAME], 'POINT (-74.9833483425103 40.05498715)')
            self.assertEqual(row['PARCEL_NUM'], '02-022-003')

class TestConformCsv(unittest.TestCase):
    "Fixture to create real files to test csv_source_to_csv()"

    # Test strings. an ASCII CSV file (with 1 row) and a Unicode CSV file,
    # along with expected outputs. These are Unicode strings; test code needs
    # to convert the input to bytes with the tested encoding.
    _ascii_header_in = u'STREETNAME,NUMBER,LATITUDE,LONGITUDE'
    _ascii_row_in = u'MAPLE ST,123,39.3,-121.2'
    _ascii_header_out = u'STREETNAME,NUMBER,{GEOM_FIELDNAME}'.format(**globals())
    _ascii_row_out = u'MAPLE ST,123,POINT (-121.2 39.3)'
    _unicode_header_in = u'STRE\u00c9TNAME,NUMBER,\u7def\u5ea6,LONGITUDE'
    _unicode_row_in = u'\u2603 ST,123,39.3,-121.2'
    _unicode_header_out = u'STRE\u00c9TNAME,NUMBER,{GEOM_FIELDNAME}'.format(**globals())
    _unicode_row_out = u'\u2603 ST,123,POINT (-121.2 39.3)'

    def setUp(self):
        self.testdir = tempfile.mkdtemp(prefix='openaddr-testPyConformCsv-')

    def tearDown(self):
        shutil.rmtree(self.testdir)

    def _convert(self, conform, src_bytes):
        "Convert a CSV source (list of byte strings) and return output as a list of unicode strings"
        self.assertNotEqual(type(src_bytes), type(u''))
        src_path = os.path.join(self.testdir, "input.csv")

        with open(src_path, "w+b") as file:
            file.write(b'\n'.join(src_bytes))

        conform = {
            "schema": 2,
            "layers": {
                "addresses": [ conform ]
            }
        }
        conform['layers']['addresses'][0]['name'] = 'default'

        conform = SourceConfig(conform, "addresses", "default")

        dest_path = os.path.join(self.testdir, "output.csv")
        csv_source_to_csv(conform, src_path, dest_path)

        with open(dest_path, 'rb') as file:
            return [s.decode('utf-8').strip() for s in file]

    def test_simple(self):
        c = { "conform": { "format": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" }, 'protocol': 'test' }
        d = (self._ascii_header_in.encode('ascii'),
             self._ascii_row_in.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

    def test_utf8(self):
        c = { "conform": { "format": "csv", "lat": u"\u7def\u5ea6", "lon": u"LONGITUDE" }, 'protocol': 'test' }
        d = (self._unicode_header_in.encode('utf-8'),
             self._unicode_row_in.encode('utf-8'))
        r = self._convert(c, d)
        self.assertEqual(self._unicode_header_out, r[0])
        self.assertEqual(self._unicode_row_out, r[1])

    def test_csvsplit(self):
        c = { "conform": { "csvsplit": ";", "format": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" }, 'protocol': 'test' }
        d = (self._ascii_header_in.replace(',', ';').encode('ascii'),
             self._ascii_row_in.replace(',', ';').encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

        unicode_conform = { "conform": { "csvsplit": u";", "format": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" }, 'protocol': 'test' }
        r = self._convert(unicode_conform, d)
        self.assertEqual(self._ascii_row_out, r[1])

    def test_csvencoded_utf8(self):
        c = { "conform": { "encoding": "utf-8", "format": "csv", "lat": u"\u7def\u5ea6", "lon": u"LONGITUDE" }, 'protocol': 'test' }
        d = (self._unicode_header_in.encode('utf-8'),
             self._unicode_row_in.encode('utf-8'))
        r = self._convert(c, d)
        self.assertEqual(self._unicode_header_out, r[0])
        self.assertEqual(self._unicode_row_out, r[1])

    def test_csvencoded_shift_jis(self):
        c = { "conform": { "encoding": "shift-jis", "format": "csv", "lat": u"\u7def\u5ea6", "lon": u"LONGITUDE" }, 'protocol': 'test' }
        d = (u'\u5927\u5b57\u30fb\u753a\u4e01\u76ee\u540d,NUMBER,\u7def\u5ea6,LONGITUDE'.encode('shift-jis'),
             u'\u6771 ST,123,39.3,-121.2'.encode('shift-jis'))
        r = self._convert(c, d)
        self.assertEqual(r[0], u'\u5927\u5b57\u30fb\u753a\u4e01\u76ee\u540d,NUMBER,{GEOM_FIELDNAME}'.format(**globals()))
        self.assertEqual(r[1], u'\u6771 ST,123,POINT (-121.2 39.3)')

    def test_headers_minus_one(self):
        c = { "conform": { "headers": -1, "format": "csv", "lon": "COLUMN4", "lat": "COLUMN3" }, 'protocol': 'test' }
        d = (u'MAPLE ST,123,39.3,-121.2'.encode('ascii'),)
        r = self._convert(c, d)
        self.assertEqual(r[0], u'COLUMN1,COLUMN2,{GEOM_FIELDNAME}'.format(**globals()))
        self.assertEqual(r[1], u'MAPLE ST,123,POINT (-121.2 39.3)')

    def test_headers_and_skiplines(self):
        c = {"conform": { "headers": 2, "skiplines": 2, "format": "csv", "lon": "LONGITUDE", "lat": "LATITUDE" }, 'protocol': 'test' }
        d = (u'HAHA,THIS,HEADER,IS,FAKE'.encode('ascii'),
             self._ascii_header_in.encode('ascii'),
             self._ascii_row_in.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

    def test_perverse_header_name_and_case(self):
        # This is an example inspired by the hipsters in us-or-portland
        # Conform says lowercase but the actual header is uppercase.
        # Also the columns are named X and Y in the input
        # 2024-11-17 (idees): Previously, this was testing that column names
        # were case-insensitive, but removed in https://github.com/openaddresses/batch-machine/pull/65
        c = {"conform": {"lon": "X", "lat": "Y", "number": "n", "street": "s", "format": "csv"}, 'protocol': 'test'}
        d = (u'n,s,X,Y'.encode('ascii'),
             u'3203,SE WOODSTOCK BLVD,-122.629314,45.479425'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(r[0], u'n,s,{GEOM_FIELDNAME}'.format(**globals()))
        self.assertEqual(r[1], u'3203,SE WOODSTOCK BLVD,POINT (-122.629314 45.479425)')

    def test_srs(self):
        # This is an example inspired by the hipsters in us-or-portland
        c = {"conform": {"lon": "X", "lat": "Y", "srs": "EPSG:2913", "number": "n", "street": "s", "format": "csv"}, 'protocol': 'test'}
        d = (u'n,s,X,Y'.encode('ascii'),
             u'3203,SE WOODSTOCK BLVD,7655634.924,668868.414'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(r[0], u'n,s,{GEOM_FIELDNAME}'.format(**globals()))

        r = r[1].split(',')
        self.assertEquals(r[0], '3203')
        self.assertEquals(r[1], 'SE WOODSTOCK BLVD')

        x,y = wkt_pt(r[2])
        self.assertAlmostEqual(-122.630842186651, x, places=4)
        self.assertAlmostEqual(45.4815543938511, y, places=4)

    def test_too_many_columns(self):
        "Check that we don't barf on input with too many columns in some rows"
        c = { "conform": { "format": "csv", "lat": "LATITUDE", "lon": "LONGITUDE" }, 'protocol': 'test' }
        d = (self._ascii_header_in.encode('ascii'),
             self._ascii_row_in.encode('ascii'),
             u'MAPLE ST,123,39.3,-121.2,EXTRY'.encode('ascii'))
        r = self._convert(c, d)
        self.assertEqual(2, len(r))
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

    def test_esri_csv(self):
        # Test that our ESRI-emitted CSV is converted correctly.
        c = { "protocol": "ESRI", "conform": { "format": "geojson", "lat": "theseare", "lon": "ignored" } }

        d = (
            u'STREETNAME,NUMBER,oa:geom'.encode('ascii'),
            u'MAPLE ST,123,POINT (-121.2 39.3)'.encode('ascii')
        )

        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])

    def test_esri_csv_no_lat_lon(self):
        # Test that the ESRI path works even without lat/lon tags. See issue #91
        c = { "protocol": "ESRI", "conform": { "format": "geojson" } }
        d = (
            u'STREETNAME,NUMBER,oa:geom'.encode('ascii'),
            u'MAPLE ST,123,POINT (-121.2 39.3)'.encode('ascii')
        )
        r = self._convert(c, d)
        self.assertEqual(self._ascii_header_out, r[0])
        self.assertEqual(self._ascii_row_out, r[1])


class TestConformTests (unittest.TestCase):

    def test_good_tests(self):
        '''
        '''
        filenames = ['cz-countrywide-good-tests.json', 'cz-countrywide-implied-tests.json']

        for filename in filenames:
            with open(os.path.join(os.path.dirname(__file__), 'sources', filename)) as file:
                source = SourceConfig(json.load(file), "addresses", "default")

            result, message = check_source_tests(source)
            self.assertIs(result, True, 'Tests should pass in {}'.format(filename))
            self.assertIsNone(message, 'No message expected from {}'.format(filename))

    def test_bad_tests(self):
        '''
        '''
        with open(os.path.join(os.path.dirname(__file__), 'sources', 'cz-countrywide-bad-tests.json')) as file:
            source = SourceConfig(json.load(file), "addresses", "default")

        result, message = check_source_tests(source)
        self.assertIs(result, False, 'Tests should fail in {}'.format(file.name))
        self.assertIn('address with /-delimited number', message, 'A message is expected from {}'.format(file.name))

    def test_no_tests(self):
        '''
        '''
        filenames = ['cz-countrywide-no-tests.json', 'cz-countrywide-disabled-tests.json']

        for filename in filenames:
            with open(os.path.join(os.path.dirname(__file__), 'sources', filename)) as file:
                source = SourceConfig(dict(json.load(file)), "addresses", "default")

            result, message = check_source_tests(source)
            self.assertIsNone(result, 'Tests should not exist in {}'.format(filename))
            self.assertIsNone(message, 'No message expected from {}'.format(filename))

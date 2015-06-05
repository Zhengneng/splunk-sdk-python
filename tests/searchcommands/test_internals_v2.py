#!/usr/bin/env python
# coding=utf-8
#
# Copyright © 2011-2015 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import unittest

from splunklib.searchcommands.internals import MetadataEncoder, ObjectView, Recorder, RecordWriter
from collections import OrderedDict
from cStringIO import StringIO

import os


class TestInternals(unittest.TestCase):

    def setUp(self):
        super(TestInternals, self).setUp()
        return

    def test_object_view(self):

        encoder = MetadataEncoder()

        d = OrderedDict([
            ('a', 1),
            ('b', 2),
            ('c', OrderedDict([
                ('d', 3),
                ('e', 4),
                ('f', OrderedDict([
                    ('g', 5),
                    ('h', 6),
                    ('i', 7)
                ])),
                ('j', 8),
                ('k', 9)
            ])),
            ('l', 10),
            ('m', 11),
            ('n', 12)
        ])

        encoding_1 = encoder.encode(d)

        v = ObjectView(d)  # Mutates d by replacing each of its dict objects with _ObjectView instances
        encoding_2 = encoder.encode(v)

        self.assertEqual(encoding_1, encoding_2)

    def test_recorder(self):
        pass

    def test_record_writer(self):
        pass

    _package_path = os.path.dirname(os.path.abspath(__file__))


if __name__ == "__main__":
    unittest.main()

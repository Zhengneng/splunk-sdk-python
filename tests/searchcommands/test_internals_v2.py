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

from __future__ import absolute_import, division, print_function, unicode_literals

from splunklib.searchcommands.internals import MetadataDecoder, MetadataEncoder, ObjectView, Recorder, RecordWriter
from cStringIO import StringIO
from tempfile import mktemp

import json
import os
import unittest


class TestInternals(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)

    def test_object_view(self):

        decoder = MetadataDecoder()
        view = decoder.decode(self._json_input)
        encoder = MetadataEncoder()
        json_output = encoder.encode(view)

        self.assertEqual(self._json_input, json_output)
        return

    def test_recorder(self):

        # Grab an input/output recording, the results of a prior countmatches run

        recording = os.path.join(self._package_path, 'recordings', 'v2', 'Splunk-6.3', 'countmatches.')

        with open(recording + 'input', 'rb') as file_1, open(recording + 'output', 'rb') as file_2:
            ifile = StringIO(file_1.read())
            result = StringIO(file_2.read())

        # Set up the input/output recorders that are under test

        ifile = Recorder(mktemp(), ifile)

        try:
            ofile = StringIO()
            ofile = Recorder(mktemp(), ofile)

            try:
                # Read and then write a line

                ifile.readline()
                ofile.write(result.readline())

                # Read and then write a block

                ifile.read()
                ofile.write(result.read())

                # Verify that what we wrote is equivalent to the original recording, the result from a prior
                # countmatches run

                self.assertEqual(ofile.getvalue(), result.getvalue())

                # Verify that we faithfully recorded the input and output files

                with open(ifile._recording.name, 'rb') as file_1, open(ofile._recording.name, 'rb') as file_2:
                    self.assertEqual(file_1.read(), ifile._file.getvalue())
                    self.assertEqual(file_2.read(), ofile._file.getvalue())
                    pass
            finally:
                ofile._recording.close()
                os.remove(ofile._recording.name)

        finally:
            ifile._recording.close()
            os.remove(ifile._recording.name)

        return

    def test_record_writer(self):
        pass

    _dictionary = {
        'a': 1,
        'b': 2,
        'c': {
            'd': 3,
            'e': 4,
            'f': {
                'g': 5,
                'h': 6,
                'i': 7
            },
            'j': 8,
            'k': 9
        },
        'l': 10,
        'm': 11,
        'n': 12
    }

    _json_input = unicode(json.dumps(_dictionary, separators=(',', ':')))
    _package_path = os.path.dirname(os.path.abspath(__file__))


if __name__ == "__main__":
    unittest.main()

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

from splunklib.searchcommands.internals import MetadataDecoder, MetadataEncoder, Recorder, RecordWriter
from collections import OrderedDict
from cStringIO import StringIO
from itertools import imap, izip
from sys import float_info, maxsize, maxunicode
from tempfile import mktemp
from time import time

import json
import os
import random
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
            ofile = Recorder(mktemp(), StringIO())

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

            finally:
                ofile._recording.close()
                os.remove(ofile._recording.name)

        finally:
            ifile._recording.close()
            os.remove(ifile._recording.name)

        return

    def test_record_writer(self):

        # Confirmed: [minint, maxint) covers the full range of values that xrange allows

        minint = (-maxsize - 1) // 2
        maxint = maxsize // 2

        max_length = 1 * 1024

        def random_integers():
            return random.sample(xrange(minint, maxint), random.randint(0, max_length))

        def random_bytes():
            return os.urandom(random.randint(0, max_length))

        def random_dict():

            # We do not call random_bytes because the JSONDecoder raises this UnicodeDecodeError when it encounters
            # bytes outside the UTF-8 character set:
            #
            #   'utf8' codec can't decode byte 0x8d in position 2: invalid start byte
            #
            # One might be tempted to select an alternative encoding, but picking one that works for all bytes is a
            # lost cause. The burden is on the customer to ensure that the strings in the dictionaries they serialize
            # contain utf-8 encoded byte strings or--better still--unicode strings. This is because the json package
            # converts all bytes strings to unicode strings before serializing them.

            return {'a': random_float(), 'b': random_unicode(), '福 酒吧': {'fu': random_float(), 'bar': random_float()}}

        def random_float():
            return random.uniform(float_info.min, float_info.max)

        def random_unicode():
            return ''.join(imap(lambda x: unichr(x), random.sample(xrange(maxunicode), random.randint(0, max_length))))

        # RecordWriter writes data in units of maxresultrows records. Default: 50,0000.
        # Partial results are written when the record count reaches maxresultrows.

        writer = RecordWriter(StringIO(), maxresultrows=10)  # small for the purposes of this unit test

        fieldnames = ['_serial', '_time', 'random_bytes', 'random_dict', 'random_integers', 'random_unicode']
        write_record = writer.write_record

        for serial_number in xrange(0, 31):
            values = [serial_number, time(), random_bytes(), random_dict(), random_unicode(), random_integers()]
            record = OrderedDict(izip(fieldnames, values))
            try:
                write_record(record)
            except Exception as error:
                self.fail(error)

        messages = [
            ('debug', random_unicode()),
            ('error', random_unicode()),
            ('fatal', random_unicode()),
            ('info', random_unicode()),
            ('warn', random_unicode())]

        for message_type, message_text in messages:
            writer.write_message(message_type, '{}', message_text)

        self.assertEqual(writer._chunk_count, 3)
        self.assertEqual(writer._record_count, 1)
        self.assertGreater(writer._buffer.tell(), 0)
        self.assertEqual(writer._total_record_count, 30)
        self.assertListEqual(writer._fieldnames, fieldnames)
        self.assertListEqual(writer._inspector['messages'], messages)

        writer.flush(finished=True)

        self.assertEqual(writer._chunk_count, 4)
        self.assertEqual(writer._record_count, 0)
        self.assertEqual(writer._buffer.tell(), 0)
        self.assertEqual(writer._buffer.getvalue(), '')
        self.assertEqual(writer._total_record_count, 31)

        self.assertRaises(RuntimeError, writer.write_record, {})
        self.assertRaises(RuntimeError, writer.flush)
        self.assertFalse(writer._ofile.closed)
        self.assertIsNone(writer._fieldnames)
        self.assertDictEqual(writer._inspector, OrderedDict())

        # RecordWriter accumulates inspector messages and metrics until maxresultrows are written.
        # RecordWriter gives consumers the ability to write partial results by calling RecordWriter.flush.
        # RecordWriter gives consumers the ability to finish early by calling RecordWriter.flush.
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

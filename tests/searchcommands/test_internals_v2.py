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
from itertools import izip
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

        # RecordWriter writes data in units of maxresultrows records. Default: 50,0000.
        # Partial results are written when the record count reaches maxresultrows.

        writer = RecordWriter(StringIO(), maxresultrows=1000)  # small for the purposes of this unit test

        fieldnames = ['_serial', '_time', 'text']
        records = [
            [0, 1380899494, 'excellent review my friend loved it yours always guppyman @GGreeny62... http://t.co/fcvq7NDHxl'],
            [1, 1380899494, 'Tú novia te ama mucho'],
            [2, 1380899494, 'RT @Cindystaysjdm: @MannyYHT girls are like the Feds, they always watching 👀'],
            [3, 1380899494, 'no me alcanza las palabras para el verbo amar..♫'],
            [4, 1380899494, '@__AmaT  요즘은 곡안쓰시고 귀농하시는군요 ㅋㅋ'],
            [5, 1380899494, 'melhor geração #DiaMundialDeRBD'],
            [6, 1380899494, '@mariam_n_k من أي ناحية مين أنا ؟ ، إذا كان السؤال هل اعرفك او لا الجواب : ل'],
            [7, 1380899494, 'Oreka Sud lance #DEMplus un logiciel de simulation du démantèlement d\'un réacteur #nucléaire http://t.co/lyC9nWxnWk'],
            [8, 1380899494, '@gusosama そんなことないですよ(｡•́︿•̀｡)でも有難うございま'],
            [9, 1380899494, '11:11 pwede pwends ta? HAHAHA']]

        # Expectation: 5 blocks of 1000 records representing partial results (5 blocks * (100 * 10 records) = 5,000)

        write_record = writer.write_record

        for i in xrange(0, 5):
            for j in xrange(0, 100):
                for record in records:
                    print(record)
                    write_record(OrderedDict(izip(fieldnames, record)))

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

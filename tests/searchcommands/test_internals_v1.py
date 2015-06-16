#!/usr/bin/env python
#
# Copyright 2011-2014 Splunk, Inc.
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

from splunklib.searchcommands.internals import CommandLineParser, InputHeader
from splunklib.searchcommands.search_command import SearchCommand

from collections import deque, OrderedDict
from contextlib import closing
from cStringIO import StringIO
from functools import wraps
from glob import iglob
from itertools import chain, ifilter, imap, izip
from json.encoder import encode_basestring as encode_string
from sys import float_info, maxsize, maxunicode
from tempfile import mktemp
from time import time
from types import MethodType

import cPickle as pickle
import json
import os
import random
import unittest


class TestInternals(unittest.TestCase):
    def setUp(self):
        unittest.TestCase.setUp(self)

    def test_command_parser(self):

        parser = CommandLineParser()
        file_path = os.path.abspath(os.path.join(self._package_path, 'data', 'input', '_empty.csv'))

        options = [
            'boolean=true',
            'duration=00:00:10',
            'fieldname=word_count',
            'file={}'.format(encode_string(file_path)),
            'integer=10',
            'optionname=foo_bar',
            'regularexpression="\\\\w+"',
            'set=foo']

        fields = ['field_1', 'field_2', 'field_3']

        command = SearchCommand()  # All options except for the builtin options are required
        parser.parse(options + fields, command)
        command_line = str(command)

        self.assertEqual(
            'stubbedstreaming boolean="t" duration="00:00:10" fieldname="word_count" file=%s integer="10" optionname="foo_bar" regularexpression="\\\\w+" set="foo" field_1 field_2 field_3' % encoder.encode(
                file_path),
            command_line)

        for option in options:
            self.assertRaises(ValueError, parser.parse,
                              [x for x in options if x != option] + ['field_1',
                                                                     'field_2',
                                                                     'field_3'],
                              command)

        command = SearchCommand()  # No options are required
        parser.parse(options + fields, command)

        for option in options:
            try:
                parser.parse(
                    [x for x in options if x != option] + ['field_1', 'field_2',
                                                           'field_3'], command)
            except Exception as e:
                self.assertFalse("Unexpected exception: %s" % e)

        try:
            parser.parse(options, command)
        except Exception as e:
            self.assertFalse("Unexpected exception: %s" % e)

        for option in command.options.itervalues():
            if option.name in ['show_configuration', 'logging_configuration',
                               'logging_level']:
                continue
            self.assertTrue(option.is_set)

        self.assertEqual(len(command.fieldnames), 0)

        try:
            parser.parse(fields, command)
        except Exception as e:
            self.assertFalse("Unexpected exception: %s" % e)

        for option in command.options.itervalues():
            self.assertFalse(option.is_set)

        self.assertListEqual(fields, command.fieldnames)
        return

    def test_command_parser_unquote(self):
        parser = CommandLineParser()

        options = [
            'foo',
            '\"foobar\"',
            '"""foobar1"""',
            '"\"foobar2\""',
            '"foo ""x"" bar"',
            '"foo \"x\" bar"',
            '"\\\\foobar"',
            '"foo \\\\ bar"',
            '"foobar\\\\"',
            'foo\\\\\\bar'
        ]

        expected = [
            'foo',
            'foobar',
            '"foobar1"',
            '"foobar2"',
            'foo "x" bar',
            'foo "x" bar',
            '\\foobar',
            'foo \\ bar',
            'foobar\\',
            'foo\\bar'
        ]

        for i in range(0, len(options)):
            print(parser.unquote(options[i]), " ", options[i])
            print(expected[i])
            self.assertEqual(expected[i], parser.unquote(options[i]))

    def test_input_header(self):

        # No items

        input_header = InputHeader()

        with closing(StringIO('\r\n'.encode())) as input_file:
            input_header.read(input_file)

        self.assertEquals(len(input_header), 0)

        # One unnamed single-line item (same as no items)

        input_header = InputHeader()

        with closing(StringIO('this%20is%20an%20unnamed%20single-line%20item\n\n'.encode())) as input_file:
            input_header.read(input_file)

        self.assertEquals(len(input_header), 0)

        input_header = InputHeader()

        with closing(StringIO('this%20is%20an%20unnamed\nmulti-\nline%20item\n\n'.encode())) as input_file:
            input_header.read(input_file)

        self.assertEquals(len(input_header), 0)

        # One named single-line item

        input_header = InputHeader()

        with closing(StringIO('Foo:this%20is%20a%20single-line%20item\n\n'.encode())) as input_file:
            input_header.read(input_file)

        self.assertEquals(len(input_header), 1)
        self.assertEquals(input_header['Foo'], 'this is a single-line item')

        input_header = InputHeader()

        with closing(StringIO('Bar:this is a\nmulti-\nline item\n\n'.encode())) as input_file:
            input_header.read(input_file)

        self.assertEquals(len(input_header), 1)
        self.assertEquals(input_header['Bar'], 'this is a\nmulti-\nline item')

        # The infoPath item (which is the path to a file that we open for reads)

        input_header = InputHeader()

        with closing(StringIO('infoPath:data/input/_empty.csv\n\n'.encode())) as input_file:
            input_header.read(input_file)

        self.assertEquals(len(input_header), 1)
        self.assertEqual(input_header['infoPath'], 'data/input/_empty.csv')

        # Set of named items

        collection = {
            'word_list': 'hello\nworld\n!',
            'word_1': 'hello',
            'word_2': 'world',
            'word_3': '!',
            'sentence': 'hello world!'}

        input_header = InputHeader()
        text = reduce(lambda value, item: value + '{}:{}\n'.format(item[0], item[1]), collection.iteritems(), '') + '\n'

        with closing(StringIO(text.encode())) as input_file:
            input_header.read(input_file)

        self.assertEqual(len(input_header), len(collection))

        for key, value in input_header.iteritems():
            self.assertEqual(value, collection[key])

        # Set of named items with an unnamed item at the beginning (the only place that an unnamed item can appear)

        with closing(StringIO(('unnamed item\n' + text).encode())) as input_file:
            input_header.read(input_file)

        self.assertEqual(len(input_header), len(collection))

        # Test iterators, indirectly through items, keys, and values

        self.assertEqual(sorted(input_header.items()), sorted(collection.items()))
        self.assertEqual(sorted(input_header.keys()), sorted(collection.keys()))
        self.assertEqual(sorted(input_header.values()), sorted(collection.values()))

        return

    _package_path = os.path.dirname(__file__)


if __name__ == "__main__":
    unittest.main()

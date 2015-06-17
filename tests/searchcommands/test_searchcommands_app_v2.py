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

from itertools import ifilter, imap, izip
from subprocess import PIPE, Popen

from __init__ import project_root

import csv
import io
import os
import unittest

class Recording(object):

    def __init__(self, path):

        self._dispatch_dir = path + '.dispatch_dir'
        self._search = None

        with io.open(os.path.join(self._dispatch_dir, 'request.csv')) as ifile:
            reader = csv.reader(ifile)
            for name, value in izip(reader.next(), reader.next()):
                if name == 'search':
                    self._search = value
                    break

        assert self._search is not None
        self._input_file = path + '.input'
        self._output_file = path + '.output'

    @property
    def dispatch_dir(self):
        return self._dispatch_dir

    @property
    def input_file(self):
        return self._input_file

    @property
    def output_file(self):
        return self._output_file

    @property
    def search(self):
        return self._search


class Recordings(object):

    def __init__(self, name, phase, protocol_version):

        basedir = Recordings._prefix + unicode(protocol_version)

        if not os.path.isdir(basedir):
            raise ValueError('Directory "{}" containing recordings for protocol version {} does not exist'.format(
                protocol_version, basedir))

        self._basedir = basedir
        self._name = name if phase is None else name + '.' + phase

    def __iter__(self):

        basedir = self._basedir
        name = self._name

        iterator = imap(
            lambda directory: Recording(os.path.join(basedir, directory, name)), ifilter(
                lambda filename: os.path.isdir(os.path.join(basedir, filename)), os.listdir(basedir)))

        return iterator

    _prefix = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'recordings', 'v')


class TestSearchCommandsApp(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)

    def test_countmatches_as_unit(self):
        expected, output = self._run_command('countmatches')
        self.assertEqual(expected, output)

    def test_generatehello_as_unit(self):
        expected, output = self._run_command('generatehello')
        # P2 [ ] TODO: Smart diff that's insensitive to _time

    @unittest.skipUnless(
        True, 'Skipping TestSearchCommandsApp.test_pypygeneratehello_as_unit because the PyPy compiler is not on the '
        'PATH.')
    def test_pypygeneratehello_as_unit(self):
        expected, output = self._run_command('pypygeneratehello')
        # P2 [ ] TODO: Smart diff that's insensitive to _time
        # P2 [ ] TODO: Skip unless pypy is the path

    def test_sum_as_unit(self):
        expected, output = self._run_command('sum', 'map')
        self.assertEqual(expected, output)
        expected, output = self._run_command('sum', 'reduce')
        self.assertEqual(expected, output)

    def _get_search_command_path(self, name):
        path = os.path.join(project_root, 'examples', 'searchcommands_app', 'package', 'bin', name + '.py')
        self.assertTrue(path)
        return path

    def _run_command(self, name, phase=None):

        command = self._get_search_command_path(name)
        args = ['splunk', 'cmd', 'python', command]

        # P2 [ ] TODO: Test against the version of Python that ships with the version of Splunk used to produce each
        # recording
        # At present we use whatever version of splunk, if any, happens to be on PATH

        # P2 [ ] TODO: Examine the contents of the app and splunklib log files (?)

        for recording in Recordings(name, phase, protocol_version=2):
            with io.open(recording.input_file, 'rb') as ifile:
                process = Popen(args, stdin=ifile, stdout=PIPE)
                output = process.stdout.read()
            with io.open(recording.output_file, 'rb') as ifile:
                expected = ifile.read()

        return expected, output

if __name__ == "__main__":
    unittest.main()

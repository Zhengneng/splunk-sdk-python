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
from unittest import main, skipUnless, TestCase

import csv
import io
import os

from tests.searchcommands import project_root


def pypy():
    process = Popen(['pypy', '--version'], stderr=PIPE, stdout=PIPE)
    output, errors = process.communicate()
    return process.returncode == 0


class Recording(object):

    def __init__(self, path):

        self._dispatch_dir = path + '.dispatch_dir'
        self._search = None

        if os.path.exists(self._dispatch_dir):
            with io.open(os.path.join(self._dispatch_dir, 'request.csv')) as ifile:
                reader = csv.reader(ifile)
                for name, value in izip(reader.next(), reader.next()):
                    if name == 'search':
                        self._search = value
                        break
            assert self._search is not None

        splunk_cmd = path + '.splunk_cmd'

        try:
            with io.open(splunk_cmd, 'rb') as f:
                self._args = f.readline().encode().split(None, 5)  # ['splunk', 'cmd', <filename>, <action>, <args>]
        except IOError as error:
            if error.errno != 2:
                raise
            self._args = ['splunk', 'cmd', 'python', None]

        self._input_file = path + '.input'
        self._output_file = path + '.output'

    def get_args(self, command_path):
        self._args[3] = command_path
        return self._args

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

    def __init__(self, name, action, phase, protocol_version):

        basedir = Recordings._prefix + unicode(protocol_version)

        if not os.path.isdir(basedir):
            raise ValueError('Directory "{}" containing recordings for protocol version {} does not exist'.format(
                protocol_version, basedir))

        self._basedir = basedir
        self._name = '.'.join(ifilter(lambda part: part is not None, (name, action, phase)))

    def __iter__(self):

        basedir = self._basedir
        name = self._name

        iterator = imap(
            lambda directory: Recording(os.path.join(basedir, directory, name)), ifilter(
                lambda filename: os.path.isdir(os.path.join(basedir, filename)), os.listdir(basedir)))

        return iterator

    _prefix = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'recordings', 'scpv')


class TestSearchCommandsApp(TestCase):

    def setUp(self):
        TestCase.setUp(self)

    def test_countmatches_as_unit(self):

        expected, output, errors, exit_status = self._run_command('countmatches', action='getinfo', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('countmatches', action='execute', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('countmatches')
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        return

    def test_generatehello_as_unit(self):

        expected, output, errors, exit_status = self._run_command('generatehello', action='getinfo', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        # self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('generatehello', action='execute', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        # self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('generatehello')
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        # self.assertEqual(expected, output)

        # P2 [ ] TODO: Smart diff that's insensitive to _time
        # P2 [ ] TODO: Smart diff that's insensitive to column order

        return

    @skipUnless(pypy(), 'Skipping TestSearchCommandsApp.test_pypygeneratetext_as_unit because pypy is not on PATH.')
    def test_pypygeneratetext_as_unit(self):

        expected, output, errors, exit_status = self._run_command('pypygeneratetext', action='getinfo', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('pypygeneratetext', action='execute', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        # self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('pypygeneratetext')
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        # self.assertEqual(expected, output)

        return

    def test_sum_as_unit(self):

        expected, output, errors, exit_status = self._run_command('sum', action='getinfo', phase='reduce', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('sum', action='getinfo', phase='map', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('sum', action='execute', phase='map', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('sum', action='execute', phase='reduce', protocol=1)
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('sum', phase='map')
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        expected, output, errors, exit_status = self._run_command('sum', phase='reduce')
        self.assertEqual(0, exit_status)
        # self.assertEqual('', errors)
        self.assertEqual(expected, output)

        return

    def _get_search_command_path(self, name):
        path = os.path.join(project_root, 'examples', 'searchcommands_app', 'package', 'bin', name + '.py')
        self.assertTrue(path)
        return path

    def _run_command(self, name, action=None, phase=None, protocol=2):

        command = self._get_search_command_path(name)

        # P2 [ ] TODO: Test against the version of Python that ships with the version of Splunk used to produce each
        # recording
        # At present we use whatever version of splunk, if any, happens to be on PATH

        # P2 [ ] TODO: Examine the contents of the app and splunklib log files (?)

        expected, output, errors, process = None, None, None, None

        for recording in Recordings(name, action, phase, protocol):
            with io.open(recording.input_file, 'rb') as ifile:
                process = Popen(recording.get_args(command), stdin=ifile, stderr=PIPE, stdout=PIPE)
                output, errors = process.communicate()
            with io.open(recording.output_file, 'rb') as ifile:
                expected = ifile.read()

        return expected, output, errors, process.returncode

if __name__ == "__main__":
    main()

# coding=utf-8
#
# Copyright 2011-2015 Splunk, Inc.
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
import __main__
import os
import sys

app_root = os.path.dirname(os.path.abspath(os.path.dirname(__main__.__file__)))


def execute(path, argv=None, environ=None):
    ExternalSearchCommand().execute(path, argv, environ)


class ExternalSearchCommand(object):
    
    def __init__(self):
        self._path = None
        self._argv = None
        self._environ = os.environ

    # region Properties

    @property
    def argv(self):
        return self._argv

    @argv.setter
    def argv(self, value):
        if not (value is None or isinstance(value, (list, tuple))):
            raise ValueError('Expected a list, tuple or value of None for environ, not {}', repr(value))
        self._argv = value

    @property
    def environ(self):
        return self._environ

    @environ.setter
    def environ(self, value):
        if not isinstance(value, dict):
            raise ValueError('Expected a dictionary value for environ, not {}', repr(value))
        self._environ = value

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        if not isinstance(value, basestring):
            raise ValueError('Expected a string value for path, not {}', repr(value))
        self._path = value

    # endregion

    # region Methods

    def execute(self, path=None, argv=None, environ=None, app_root=None):

        try:
            if path is not None:
                self.path = path
            if argv is not None:
                self.argv = argv
            if environ is not None:
                self.environ = environ
            os.execvpe(self._path, self._argv, self._environ)
        except Exception as error:
            print('{} execution error: {}'.format(self.__class__.__name__, error), file=sys.stderr)

    # endregion

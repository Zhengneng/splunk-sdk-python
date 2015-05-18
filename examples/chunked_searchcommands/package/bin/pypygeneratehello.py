#!/usr/bin/env python
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
import app
import os
import sys

print('pypygeneratehello command started', file=sys.stderr)

from splunklib.searchcommands.external_search_command import app_root, execute
os.environ.pop('DYLD_LIBRARY_PATH', None)
execute('pypy', ('pypy', os.path.join(app_root, 'bin', 'generatehello.py')))

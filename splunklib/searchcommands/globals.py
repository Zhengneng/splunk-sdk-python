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
# distributed under the License is distributed on an "AS IS" BASI, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, unicode_literals

from collections import namedtuple
from os.path import abspath, dirname
import sys

from .internals import configure_logging

app_root = dirname(abspath(dirname(sys.modules['__main__'].__dict__.get('__file__', 'Python shell'))))
splunklib_logger = configure_logging('splunklib', app_root)[0]

SearchMetric = namedtuple(b'Metric', (b'elapsed_seconds', b'invocation_count', b'input_count', b'output_count'))

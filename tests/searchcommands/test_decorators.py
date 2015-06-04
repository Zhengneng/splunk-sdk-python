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

from splunklib.searchcommands.search_command import SearchCommand
from splunklib.searchcommands import Configuration

import os
import unittest

# TODO: Test a variety of configuration settings
@Configuration()
class StubbedSearchCommand(SearchCommand):

    # TODO: Test a variety of option types

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        @classmethod
        def fix_up(cls, command_class):
            pass

class TestDecorators(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)

    _package_directory = os.path.dirname(__file__)


if __name__ == "__main__":
    unittest.main()

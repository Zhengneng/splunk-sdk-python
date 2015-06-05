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
from splunklib.searchcommands import Configuration, Option, validators

import os
import unittest

# [O] TODO: Test a variety of configuration settings

@Configuration()
class StubbedSearchCommand(SearchCommand):

    boolean = Option(
        doc='''
        **Syntax:** **boolean=***<value>*
        **Description:** A boolean value''',
        validate=validators.Boolean())

    required_boolean = Option(
        doc='''
        **Syntax:** **boolean=***<value>*
        **Description:** A boolean value''',
        require=True, validate=validators.Boolean())

    duration = Option(
        doc='''
        **Syntax:** **duration=***<value>*
        **Description:** A length of time''',
        validate=validators.Duration())

    required_duration = Option(
        doc='''
        **Syntax:** **duration=***<value>*
        **Description:** A length of time''',
        require=True, validate=validators.Duration())

    fieldname = Option(
        doc='''
        **Syntax:** **fieldname=***<value>*
        **Description:** Name of a field''',
        validate=validators.Fieldname())

    required_fieldname = Option(
        doc='''
        **Syntax:** **fieldname=***<value>*
        **Description:** Name of a field''',
        require=True, validate=validators.Fieldname())

    file = Option(
        doc='''
        **Syntax:** **file=***<value>*
        **Description:** Name of a file''',
        validate=validators.File())

    required_file = Option(
        doc='''
        **Syntax:** **file=***<value>*
        **Description:** Name of a file''',
        require=True, validate=validators.File())

    integer = Option(
        doc='''
        **Syntax:** **integer=***<value>*
        **Description:** An integer value''',
        validate=validators.Integer())

    required_integer = Option(
        doc='''
        **Syntax:** **integer=***<value>*
        **Description:** An integer value''',
        require=True, validate=validators.Integer())

    map = Option(
        doc='''
        **Syntax:** **map=***<value>*
        **Description:** A mapping from one value to another''',
        validate=validators.Map(foo=1, bar=2, test=3))

    required_map = Option(
        doc='''
        **Syntax:** **map=***<value>*
        **Description:** A mapping from one value to another''',
        require=True, validate=validators.Map(foo=1, bar=2, test=3))

    optionname = Option(
        doc='''
        **Syntax:** **optionname=***<value>*
        **Description:** The name of an option (used internally)''',
        validate=validators.OptionName())

    required_optionname = Option(
        doc='''
        **Syntax:** **optionname=***<value>*
        **Description:** The name of an option (used internally)''',
        require=True, validate=validators.OptionName())

    regularexpression = Option(
        doc='''
        **Syntax:** **regularexpression=***<value>*
        **Description:** Regular expression pattern to match''',
        validate=validators.RegularExpression())

    required_regularexpression = Option(
        doc='''
        **Syntax:** **regularexpression=***<value>*
        **Description:** Regular expression pattern to match''',
        require=True, validate=validators.RegularExpression())

    set = Option(
        doc='''
        **Syntax:** **set=***<value>*
        **Description:** A member of a set''',
        validate=validators.Set('foo', 'bar', 'test'))

    required_set = Option(
        doc='''
        **Syntax:** **set=***<value>*
        **Description:** A member of a set''',
        require=True, validate=validators.Set('foo', 'bar', 'test'))

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        @classmethod
        def fix_up(cls, command_class):
            pass

class TestDecorators(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)

    def test_option(self):

        command = StubbedSearchCommand()
        command.options.reset()
        missing = command.options.get_missing()
        self.assertListEqual(missing, [option for option in command.options if option.require is True])

        error_count = 0
        args = []

        for arg in args:
            result = arg.split('=', 1)
            if len(result) == 1:
                command.fieldnames.append(result[0])
            else:
                name, value = result
                try:
                    option = command.options[name]
                except KeyError:
                    command.write_error('Unrecognized option: {}={}'.format(name, value))
                    error_count += 1
                    continue
                try:
                    option.value = value
                except ValueError:
                    command.write_error('Illegal value: {}'.format(option))
                    error_count += 1
                    continue
            pass
        pass

        missing = command.options.get_missing()

    _package_directory = os.path.dirname(__file__)


if __name__ == "__main__":
    unittest.main()

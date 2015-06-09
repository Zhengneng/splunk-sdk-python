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

from splunklib.searchcommands import Configuration, Option, validators
from splunklib.searchcommands.search_command import SearchCommand

from unittest import TestCase
import os
import sys

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

class TestDecorators(TestCase):

    def setUp(self):
        TestCase.setUp(self)

    def test_configuration(self):

        for setting, values in (
            ('clear_required_fields',
             (True, False)),
            ('distributed',
             (True, False)),
            ('generates_timeorder',
             (True, False)),
            ('generating',
             (True, False)),
            ('maxinputs',
             (0, 50000, sys.maxsize)),
            ('overrides_timeorder',
             (True, False)),
            ('required_fields',
             (['field_1', 'field_2'], {'field_1', 'field_2'}, ('field_1', 'field_2'))),
            ('requires_preop',
             (True, False)),
            ('retainsevents',
             (True, False)),
            ('run_in_preview',
              (True, False)),
            ('streaming',
             (True, False)),
            ('streaming_preop',
             ('some unicode string', b'some byte string')),
            ('type',
             ('events', 'reporting', 'streaming', b'events', b'reporting', b'streaming'))):
            for value in values:
                @Configuration(**{setting: value})
                class ConfiguredSearchCommand(SearchCommand):
                    pass

    def test_option(self):

        presets = ['logging_level="WARNING"', 'record="f"', 'show_configuration="f"']

        command = StubbedSearchCommand()
        options = command.options
        itervalues = options.itervalues

        options.reset()
        missing = options.get_missing()
        self.assertListEqual(missing, [option.name for option in itervalues() if option.is_required])
        self.assertListEqual(presets, [str(option) for option in itervalues() if option.value is not None])
        self.assertListEqual(presets, [str(option) for option in itervalues() if str(option) != option.name + '=null'])

        test_option_values = {
            validators.Boolean: ('0', 'non-boolean value'),
            validators.Duration: ('24:59:59', 'non-duration value'),
            validators.Fieldname: ('some.field_name', 'non-fieldname value'),
            validators.File: (__file__, 'non-existent file'),
            validators.Integer: ('100', 'non-integer value'),
            validators.List: ('a,b,c', '"non-list value'),
            validators.Map: ('foo', 'non-existent map entry'),
            validators.OptionName: ('some_option_name', 'non-option name value'),
            validators.RegularExpression: ('\\s+', '(poorly formed regular expression'),
            validators.Set: ('bar', 'non-existent set entry')}

        for option in itervalues():
            validator = option.validator

            if validator is None:
                # TODO: Consider adding validators for these two
                self.assertIn(option.name, ['logging_configuration', 'logging_level'])
                continue

            legal_value, illegal_value = test_option_values[type(validator)]
            option.value = legal_value

            self.assertEqual(
                validator.format(option.value), validator.format(validator.__call__(legal_value)),
                "{}={}".format(option.name, legal_value))

            try:
                option.value = illegal_value
            except ValueError:
                pass
            except BaseException as error:
                self.assertFalse('Expected ValueError for {}={}, not this {}: {}'.format(
                    option.name, illegal_value, type(error).__name__, error))
            else:
                self.assertFalse('Expected ValueError for {}={}, not a pass.'.format(option.name, illegal_value))

        return

    _package_directory = os.path.dirname(__file__)


if __name__ == "__main__":
    unittest.main()

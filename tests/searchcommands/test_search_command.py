#!/usr/bin/env python
#
# Copyright 2011-2014 Splunk, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"): you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, unicode_literals

from splunklib.searchcommands import Configuration, StreamingCommand
from splunklib.searchcommands.decorators import ConfigurationSetting, Option
from splunklib.searchcommands.search_command import SearchCommand
from splunklib.client import Service
from cStringIO import StringIO
from json.encoder import encode_basestring as encode_string
from unittest import main, TestCase

import os
import re


@Configuration()
class TestCommand(SearchCommand):

    required_option_1 = Option(require=True)
    required_option_2 = Option(require=True)

    def echo(self, records):
        for record in records:
            yield record

    def _execute(self, ifile, process):
        SearchCommand._execute(self, ifile, self.echo)

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):

        # region SCP v1/v2 properties

        generating = ConfigurationSetting()
        required_fields = ConfigurationSetting()
        streaming_preop = ConfigurationSetting()

        # endregion

        # region SCP v1 properties

        clear_required_fields = ConfigurationSetting()
        generates_timeorder = ConfigurationSetting()
        overrides_timeorder = ConfigurationSetting()
        requires_preop = ConfigurationSetting()
        retainsevents = ConfigurationSetting()
        streaming = ConfigurationSetting()

        # endregion

        # region SCP v2 properties

        distributed = ConfigurationSetting()
        maxinputs = ConfigurationSetting()
        run_in_preview = ConfigurationSetting()
        type = ConfigurationSetting()

        # endregion

@Configuration()
class TestStreamingCommand(StreamingCommand):
    def stream(self, records):
        serial_number = 0L
        for record in records:
            action = record['action']
            if action == 'raise_error':
                raise RuntimeError('Testing')
            value = self.search_results_info if action == 'get_search_results_info' else None
            yield {'_serial': serial_number, 'data': value}
            serial_number += 1L
        return


class TestSearchCommand(TestCase):
    def setUp(self):
        TestCase.setUp(self)

    def test_process_scpv1(self):

        # TestCommand.process should complain if supports_getinfo == False
        # We support dynamic configuration, not static

        # The exception line number may change, so we're using a regex match instead of a string match

        expected = re.compile(
            r'error_message=RuntimeError at ".+search_command\.py", line \d\d\d : Command test appears to be '
            r'statically configured for search command protocol version 1 and static configuration is unsupported by '
            r'splunklib.searchcommands. Please ensure that default/commands.conf contains this stanza:\n'
            r'\[test\]\n'
            r'filename = test.py\n'
            r'enableheader = true\n'
            r'outputheader = true\n'
            r'requires_srinfo = true\n'
            r'supports_getinfo = true\n'
            r'supports_multivalues = true\n'
            r'supports_rawargs = true')

        argv = ['test.py', 'not__GETINFO__or__EXECUTE__', 'option=value', 'fieldname']
        command = TestCommand()
        result = StringIO()

        self.assertRaises(SystemExit, command.process, argv, ofile=result)
        self.assertRegexpMatches(result.getvalue(), expected)

        # TestCommand.process should return configuration settings on Getinfo probe

        argv = ['test.py', '__GETINFO__', 'required_option_1=value', 'required_option_2=value']
        command = TestCommand()
        ifile = StringIO('\n')
        result = StringIO()

        self.assertEqual(str(command.configuration), '')

        self.assertEqual(
            repr(command.configuration),
            "[(u'clear_required_fields', None, [1]), (u'distributed', None, [2]), (u'generates_timeorder', None, [1]), "
            "(u'generating', None, [1, 2]), (u'maxinputs', None, [2]), (u'overrides_timeorder', None, [1]), "
            "(u'required_fields', None, [1, 2]), (u'requires_preop', None, [1]), (u'retainsevents', None, [1]), "
            "(u'run_in_preview', None, [2]), (u'streaming', None, [1]), (u'streaming_preop', None, [1, 2]), "
            "(u'type', None, [2])]")

        try:
            # noinspection PyTypeChecker
            command.process(argv, ifile, ofile=result)
        except BaseException as error:
            self.fail('{0}: {1}: {2}\n'.format(type(error).__name__, error, result.getvalue()))

        self.assertEqual('\r\n\r\n\r\n', result.getvalue())  # No message header and no configuration settings

        ifile = StringIO('\n')
        result = StringIO()

        # We might also put this sort of code into our SearchCommand.prepare override ...

        configuration = command.configuration

        # SCP v1/v2 configuration settings
        configuration.generating = True
        configuration.required_fields = ['*']
        configuration.streaming_preop = 'some streaming command'

        # SCP v1 configuration settings
        configuration.clear_required_fields = True
        configuration.generates_timeorder = True
        configuration.overrides_timeorder = True
        configuration.requires_preop = True
        configuration.retainsevents = True
        configuration.streaming = True

        # SCP v2 configuration settings (SCP v1 requires that maxinputs and run_in_preview are set in commands.conf)
        configuration.distributed = True
        configuration.maxinputs = 50000
        configuration.run_in_preview = True
        configuration.type = 'streaming'

        self.assertEqual(
            str(command.configuration),
            'clear_required_fields="True", generates_timeorder="True", generating="True", overrides_timeorder="True", '
            'required_fields="[u\'*\']", requires_preop="True", retainsevents="True", streaming="True", '
            'streaming_preop="some streaming command"')

        self.assertEqual(
            repr(command.configuration),
            "[(u'clear_required_fields', True, [1]), (u'distributed', True, [2]), (u'generates_timeorder', True, [1]), "
            "(u'generating', True, [1, 2]), (u'maxinputs', 50000, [2]), (u'overrides_timeorder', True, [1]), "
            "(u'required_fields', [u'*'], [1, 2]), (u'requires_preop', True, [1]), (u'retainsevents', True, [1]), "
            "(u'run_in_preview', True, [2]), (u'streaming', True, [1]), "
            "(u'streaming_preop', u'some streaming command', [1, 2]), (u'type', u'streaming', [2])]")

        try:
            # noinspection PyTypeChecker
            command.process(argv, ifile, ofile=result)
        except BaseException as error:
            self.fail('{0}: {1}: {2}\n'.format(type(error).__name__, error, result.getvalue()))

        expected = (
            '\r\n'
            'clear_required_fields,__mv_clear_required_fields,generates_timeorder,__mv_generates_timeorder,generating,'
            '__mv_generating,overrides_timeorder,__mv_overrides_timeorder,required_fields,__mv_required_fields,'
            'requires_preop,__mv_requires_preop,retainsevents,__mv_retainsevents,streaming,__mv_streaming,'
            'streaming_preop,__mv_streaming_preop\r\n'
            '1,,1,,1,,1,,*,,1,,1,,1,,some streaming command,\r\n')

        self.assertEqual(expected, result.getvalue())  # No message header and no configuration settings

        for action in '__GETINFO__', '__EXECUTE__':

            # TestCommand.process should produce an error record on parser errors

            argv = [
                'test.py', action, 'required_option_1=value', 'required_option_2=value', 'undefined_option=value',
                'fieldname_1', 'fieldname_2']

            command = TestCommand()
            ifile = StringIO('\n')
            result = StringIO()

            self.assertRaises(SystemExit, command.process, argv, ifile, ofile=result)
            self.assertTrue(
                'error_message=Unrecognized test command option: undefined_option="value"\r\n\r\n',
                result.getvalue())

            # TestCommand.process should produce an error record when required options are missing

            argv = ['test.py', action, 'required_option_2=value', 'fieldname_1']
            command = TestCommand()
            ifile = StringIO('\n')
            result = StringIO()

            self.assertRaises(SystemExit, command.process, argv, ifile, ofile=result)

            self.assertTrue(
                'error_message=A value for test command option required_option_1 is required\r\n\r\n',
                result.getvalue())

            argv = ['test.py', action, 'fieldname_1']
            command = TestCommand()
            ifile = StringIO('\n')
            result = StringIO()

            self.assertRaises(SystemExit, command.process, argv, ifile, ofile=result)

            self.assertTrue(
                'error_message=Values for these test command options are required: required_option_1, required_option_2'
                '\r\n\r\n',
                result.getvalue())

        # TestStreamingCommand.process should exit on processing exceptions

        ifile = StringIO('\naction\r\nraise_error\r\n')
        argv = ['test.py', '__EXECUTE__']
        command = TestStreamingCommand()
        result = StringIO()

        try:
            # noinspection PyTypeChecker
            command.process(argv, ifile, ofile=result)
        except SystemExit as error:
            self.assertNotEqual(error.code, 0)
            self.assertRegexpMatches(
                result.getvalue(),
                r'^error_message=RuntimeError at ".+", line \d+ : Testing\r\n\r\n$')
        except BaseException as error:
            self.fail('Expected SystemExit, but caught {}: {}'.format(type(error).__name__, error))
        else:
            self.fail('Expected SystemExit, but no exception was raised')

        # Command.process should provide access to search results info
        info_path = os.path.join(
            self._package_directory, 'recordings', 'scpv1', 'Splunk-6.3', 'countmatches.execute.dispatch_dir',
            'externSearchResultsInfo.csv')

        ifile = StringIO('infoPath:' + info_path + '\n\naction\r\nget_search_results_info\r\n')
        argv = ['test.py', '__EXECUTE__']
        command = TestStreamingCommand()
        result = StringIO()

        try:
            # noinspection PyTypeChecker
            command.process(argv, ifile, ofile=result)
        except BaseException as error:
            self.fail('Expected no exception, but caught {}: {}'.format(type(error).__name__, error))
        else:
            self.assertRegexpMatches(
                result.getvalue(),
                r'^\r\n'
                r'data,__mv_data,_serial,__mv__serial\r\n'
                r'"{u\'is_summary_index\': 0, u\'normalized_search\': \'\', u\'rt_backfill\': 0, u\'rtspan\': \'\', '
                r'.+'
                r'\r\n$')

        # TestStreamingCommand.process should provide access to a service object when search results info is available

        self.assertIsInstance(command.service, Service)

        self.assertEqual(command.service.authority,
                         command.search_results_info.splunkd_uri)

        self.assertEqual(command.service.scheme,
                         command.search_results_info.splunkd_protocol)

        self.assertEqual(command.service.port,
                         command.search_results_info.splunkd_port)

        self.assertEqual(command.service.token,
                         command.search_results_info.auth_token)

        self.assertEqual(command.service.namespace.app,
                         command.search_results_info.ppc_app)

        self.assertEqual(command.service.namespace.owner,
                         None)
        self.assertEqual(command.service.namespace.sharing,
                         None)

        # Command.process should not provide access to search results info or a service object when the 'infoPath'
        # input header is unavailable

        ifile = StringIO('\naction\r\nget_search_results_info')
        argv = ['teststreaming.py', '__EXECUTE__']
        command = TestStreamingCommand()

        # noinspection PyTypeChecker
        command.process(argv, ifile, ofile=result)

        self.assertIsNone(command.search_results_info)
        self.assertIsNone(command.service)

        return

    def test_process_scpv2(self):

        # SearchCommand.process should
        #
        # 1. Recognize all standard options:

        metadata = (
            '{{'
                '"action": "getinfo", "preview": false, "searchinfo": {{'
                    '"latest_time": "0",'
                    '"splunk_version": "20150522",'
                    '"username": "admin",'
                    '"app": "searchcommands_app",'
                    '"args": ['
                        '"logging_configuration={logging_configuration}",'
                        '"logging_level={logging_level}",'
                        '"record={record}",'
                        '"show_configuration={show_configuration}",'
                        '"required_option_1=value_1",'
                        '"required_option_2=value_2"'
                    '],'
                    '"search": "%7C%20inputlookup%20tweets%20%7C%20countmatches%20fieldname%3Dword_count%20pattern%3D%22%5Cw%2B%22%20text%20record%3Dt%20%7C%20export%20add_timestamp%3Df%20add_offset%3Dt%20format%3Dcsv%20segmentation%3Draw",'
                    '"earliest_time": "0",'
                    '"session_key": "0JbG1fJEvXrL6iYZw9y7tmvd6nHjTKj7ggaE7a4Jv5R0UIbeYJ65kThn^3hiNeoqzMT_LOtLpVR3Y8TIJyr5bkHUElMijYZ8l14wU0L4n^Oa5QxepsZNUIIQCBm^",'
                    '"owner": "admin",'
                    '"sid": "1433261372.158",'
                    '"splunkd_uri": "https://127.0.0.1:8089",'
                    '"dispatch_dir": {dispatch_dir},'
                    '"raw_args": ['
                        '"logging_configuration={logging_configuration}",'
                        '"logging_level={logging_level}",'
                        '"record={record}",'
                        '"show_configuration={show_configuration}",'
                        '"required_option_1=value_1",'
                        '"required_option_2=value_2"'
                    ']'
                '}}'
            '}}')

        basedir = self._package_directory

        dispatch_dir = os.path.join(basedir, 'recordings', 'scpv2', 'Splunk-6.3', 'countmatches.dispatch_dir')
        logging_configuration = os.path.join(basedir, 'apps', 'app_with_logging_configuration', 'default', 'logging.conf')
        logging_level = 'WARNING'
        record = True
        show_configuration = True

        getinfo_metadata = metadata.format(
            dispatch_dir=encode_string(dispatch_dir),
            logging_configuration=logging_configuration,
            logging_level=logging_level,
            record=('true' if record is True else 'false'),
            show_configuration=('true' if show_configuration is True else 'false'))

        execute_metadata = '{"action":"execute","finished":true}'
        execute_body = 'test\r\ndata\r\n'

        ifile = StringIO(
            'chunked 1.0,{},0\n{}'.format(len(getinfo_metadata), getinfo_metadata) +
            'chunked 1.0,{},{}\n{}{}'.format(len(execute_metadata), len(execute_body), execute_metadata, execute_body))

        command = TestCommand()
        result = StringIO()
        argv = ['test.py']

        try:
            # noinspection PyTypeChecker
            command.process(argv, ifile, ofile=result)
        except SystemExit as error:
            self.fail('Unexpected exception: {}: {}'.format(type(error).__name__, error))

        self.assertEqual(command.logging_configuration, logging_configuration)
        self.assertEqual(command.logging_level, logging_level)
        self.assertEqual(command.record, record)
        self.assertEqual(command.show_configuration, show_configuration)
        self.assertEqual(command.required_option_1, 'value_1')
        self.assertEqual(command.required_option_2, 'value_2')

        self.assertEqual(
            'chunked 1.0,68,0\n'
            '{"inspector":{"messages":[["INFO","test command configuration: "]]}}\n'
            'chunked 1.0,17,23\n'
            '{"finished":true}test,__mv_test\r\n'
            'data,\r\n',
            result.getvalue())

        # 2. Produce an error message, log a debug message, and exit when invalid standard option values are encountered
        # 3. Produce an error message, log an error message that includes a traceback, and exit when an exception is
        #    raised during command execution.
        # 4. Provide access to these properties
        #   fieldnames
        #   metadata
        #   search_results_info
        #   service

        # TestCommand.process should complain if supports_getinfo == False
        # We support dynamic configuration, not static

        # The exception line number may change, so we're using a regex match instead of a string match

        expected = re.compile(
            r'error_message=RuntimeError at ".+search_command\.py", line \d\d\d : Command test appears to be '
            r'statically configured for search command protocol version 1 and static configuration is unsupported by '
            r'splunklib.searchcommands. Please ensure that default/commands.conf contains this stanza:\n'
            r'\[test\]\n'
            r'filename = test.py\n'
            r'enableheader = true\n'
            r'outputheader = true\n'
            r'requires_srinfo = true\n'
            r'supports_getinfo = true\n'
            r'supports_multivalues = true\n'
            r'supports_rawargs = true')

        argv = ['test.py', 'not__GETINFO__or__EXECUTE__', 'option=value', 'fieldname']
        command = TestCommand()
        result = StringIO()

        self.assertRaises(SystemExit, command.process, argv, ofile=result)
        self.assertRegexpMatches(result.getvalue(), expected)

        # TestCommand.process should return configuration settings on Getinfo probe

        argv = ['test.py', '__GETINFO__', 'required_option_1=value', 'required_option_2=value']
        command = TestCommand()
        ifile = StringIO('\n')
        result = StringIO()

        self.assertEqual(str(command.configuration), '')

        self.assertEqual(
            repr(command.configuration),
            "[(u'clear_required_fields', None, [1]), (u'distributed', None, [2]), (u'generates_timeorder', None, [1]), "
            "(u'generating', None, [1, 2]), (u'maxinputs', None, [2]), (u'overrides_timeorder', None, [1]), "
            "(u'required_fields', None, [1, 2]), (u'requires_preop', None, [1]), (u'retainsevents', None, [1]), "
            "(u'run_in_preview', None, [2]), (u'streaming', None, [1]), (u'streaming_preop', None, [1, 2]), "
            "(u'type', None, [2])]")

        try:
            # noinspection PyTypeChecker
            command.process(argv, ifile, ofile=result)
        except BaseException as error:
            self.fail('{0}: {1}: {2}\n'.format(type(error).__name__, error, result.getvalue()))

        self.assertEqual('\r\n\r\n\r\n', result.getvalue())  # No message header and no configuration settings

        ifile = StringIO('\n')
        result = StringIO()

        # We might also put this sort of code into our SearchCommand.prepare override ...

        configuration = command.configuration

        # SCP v1/v2 configuration settings
        configuration.generating = True
        configuration.required_fields = ['*']
        configuration.streaming_preop = 'some streaming command'

        # SCP v1 configuration settings
        configuration.clear_required_fields = True
        configuration.generates_timeorder = True
        configuration.overrides_timeorder = True
        configuration.requires_preop = True
        configuration.retainsevents = True
        configuration.streaming = True

        # SCP v2 configuration settings (SCP v1 requires that maxinputs and run_in_preview are set in commands.conf)
        configuration.distributed = True
        configuration.maxinputs = 50000
        configuration.run_in_preview = True
        configuration.type = 'streaming'

        self.assertEqual(
            str(command.configuration),
            'clear_required_fields="True", generates_timeorder="True", generating="True", overrides_timeorder="True", '
            'required_fields="[u\'*\']", requires_preop="True", retainsevents="True", streaming="True", '
            'streaming_preop="some streaming command"')

        self.assertEqual(
            repr(command.configuration),
            "[(u'clear_required_fields', True, [1]), (u'distributed', True, [2]), (u'generates_timeorder', True, [1]), "
            "(u'generating', True, [1, 2]), (u'maxinputs', 50000, [2]), (u'overrides_timeorder', True, [1]), "
            "(u'required_fields', [u'*'], [1, 2]), (u'requires_preop', True, [1]), (u'retainsevents', True, [1]), "
            "(u'run_in_preview', True, [2]), (u'streaming', True, [1]), "
            "(u'streaming_preop', u'some streaming command', [1, 2]), (u'type', u'streaming', [2])]")

        try:
            # noinspection PyTypeChecker
            command.process(argv, ifile, ofile=result)
        except BaseException as error:
            self.fail('{0}: {1}: {2}\n'.format(type(error).__name__, error, result.getvalue()))

        expected = (
            '\r\n'
            'clear_required_fields,__mv_clear_required_fields,generates_timeorder,__mv_generates_timeorder,generating,'
            '__mv_generating,overrides_timeorder,__mv_overrides_timeorder,required_fields,__mv_required_fields,'
            'requires_preop,__mv_requires_preop,retainsevents,__mv_retainsevents,streaming,__mv_streaming,'
            'streaming_preop,__mv_streaming_preop\r\n'
            '1,,1,,1,,1,,*,,1,,1,,1,,some streaming command,\r\n')

        self.assertEqual(expected, result.getvalue())  # No message header and no configuration settings

        for action in '__GETINFO__', '__EXECUTE__':

            # TestCommand.process should produce an error record on parser errors

            argv = [
                'test.py', action, 'required_option_1=value', 'required_option_2=value', 'undefined_option=value',
                'fieldname_1', 'fieldname_2']

            command = TestCommand()
            ifile = StringIO('\n')
            result = StringIO()

            self.assertRaises(SystemExit, command.process, argv, ifile, ofile=result)
            self.assertTrue(
                'error_message=Unrecognized test command option: undefined_option="value"\r\n\r\n',
                result.getvalue())

            # TestCommand.process should produce an error record when required options are missing

            argv = ['test.py', action, 'required_option_2=value', 'fieldname_1']
            command = TestCommand()
            ifile = StringIO('\n')
            result = StringIO()

            self.assertRaises(SystemExit, command.process, argv, ifile, ofile=result)

            self.assertTrue(
                'error_message=A value for test command option required_option_1 is required\r\n\r\n',
                result.getvalue())

            argv = ['test.py', action, 'fieldname_1']
            command = TestCommand()
            ifile = StringIO('\n')
            result = StringIO()

            self.assertRaises(SystemExit, command.process, argv, ifile, ofile=result)

            self.assertTrue(
                'error_message=Values for these test command options are required: required_option_1, required_option_2'
                '\r\n\r\n',
                result.getvalue())

        # TestStreamingCommand.process should exit on processing exceptions

        ifile = StringIO('\naction\r\nraise_error\r\n')
        argv = ['test.py', '__EXECUTE__']
        command = TestStreamingCommand()
        result = StringIO()

        try:
            # noinspection PyTypeChecker
            command.process(argv, ifile, ofile=result)
        except SystemExit as error:
            self.assertNotEqual(error.code, 0)
            self.assertRegexpMatches(
                result.getvalue(),
                r'^error_message=RuntimeError at ".+", line \d+ : Testing\r\n\r\n$')
        except BaseException as error:
            self.fail('Expected SystemExit, but caught {}: {}'.format(type(error).__name__, error))
        else:
            self.fail('Expected SystemExit, but no exception was raised')

        # Command.process should provide access to search results info
        info_path = os.path.join(
            self._package_directory, 'recordings', 'scpv1', 'Splunk-6.3', 'countmatches.execute.dispatch_dir',
            'externSearchResultsInfo.csv')

        ifile = StringIO('infoPath:' + info_path + '\n\naction\r\nget_search_results_info\r\n')
        argv = ['test.py', '__EXECUTE__']
        command = TestStreamingCommand()
        result = StringIO()

        try:
            # noinspection PyTypeChecker
            command.process(argv, ifile, ofile=result)
        except BaseException as error:
            self.fail('Expected no exception, but caught {}: {}'.format(type(error).__name__, error))
        else:
            self.assertRegexpMatches(
                result.getvalue(),
                r'^\r\n'
                r'data,__mv_data,_serial,__mv__serial\r\n'
                r'"{u\'is_summary_index\': 0, u\'normalized_search\': \'\', u\'rt_backfill\': 0, u\'rtspan\': \'\', '
                r'.+'
                r'\r\n$')

        # TestStreamingCommand.process should provide access to a service object when search results info is available

        self.assertIsInstance(command.service, Service)

        self.assertEqual(command.service.authority,
                         command.search_results_info.splunkd_uri)

        self.assertEqual(command.service.scheme,
                         command.search_results_info.splunkd_protocol)

        self.assertEqual(command.service.port,
                         command.search_results_info.splunkd_port)

        self.assertEqual(command.service.token,
                         command.search_results_info.auth_token)

        self.assertEqual(command.service.namespace.app,
                         command.search_results_info.ppc_app)

        self.assertEqual(command.service.namespace.owner,
                         None)
        self.assertEqual(command.service.namespace.sharing,
                         None)

        # Command.process should not provide access to search results info or a service object when the 'infoPath'
        # input header is unavailable

        ifile = StringIO('\naction\r\nget_search_results_info')
        argv = ['teststreaming.py', '__EXECUTE__']
        command = TestStreamingCommand()

        # noinspection PyTypeChecker
        command.process(argv, ifile, ofile=result)

        self.assertIsNone(command.search_results_info)
        self.assertIsNone(command.service)

        return

    _package_directory = os.path.dirname(os.path.abspath(__file__))


if __name__ == "__main__":
    main()

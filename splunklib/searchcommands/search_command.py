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

# Absolute imports

from splunklib.client import Service

from collections import OrderedDict
from cStringIO import StringIO
from inspect import getmembers
from itertools import imap, izip
from logging import _levelNames, getLevelName
from os import environ, path
from sys import argv, exit, stdin, stdout
from urlparse import urlsplit
from xml.etree import ElementTree

import sys
import re
import csv
import json

# Relative imports

from . import logging, splunk_csv
from .decorators import Option
from .validators import Boolean, Fieldname
from .search_command_internals import InputHeader, MessagesHeader, SearchCommandParser


class SearchCommand(object):
    """ Represents a custom search command.

    """
    def __init__(self, app_root=None):
        """
        :param app_root: The root of the application directory, used primarily by tests.
        :type app_root: str or NoneType
        """

        # Variables that may be used, but not altered by derived classes

        self.logger, self._logging_configuration = logging.configure(type(self).__name__, app_root=app_root)
        self.input_header = InputHeader()
        self.messages = MessagesHeader()

        if 'SPLUNK_HOME' not in environ:
            self.logger.warning(
                'SPLUNK_HOME environment variable is undefined.\n'
                'If you are testing outside of Splunk, consider running under control of the Splunk CLI:\n'
                '    splunk cmd %s\n'
                'If you are running inside of Splunk, SPLUNK_HOME should be defined. Consider troubleshooting your '
                'installation.', self)

        # Variables backing option/property values

        self._app_root = app_root
        self._configuration = None
        self._fieldnames = None
        self._option_view = None
        self._output_file = None
        self._search_results_info = None
        self._service = None

        # Internal variables

        self._default_logging_level = self.logger.level
        self._message_count = None

        self.parser = SearchCommandParser()

    def __repr__(self):
        return str(self)

    def __str__(self):
        values = [type(self).name, str(self.options)] + self.fieldnames
        text = ' '.join([value for value in values if len(value) > 0])
        return text

    # region Options

    @Option
    def logging_configuration(self):
        """ **Syntax:** logging_configuration=<path>

        **Description:** Loads an alternative logging configuration file for
        a command invocation. The logging configuration file must be in Python
        ConfigParser-format. Path names are relative to the app root directory.

        """
        return self._logging_configuration

    @logging_configuration.setter
    def logging_configuration(self, value):
        self.logger, self._logging_configuration = logging.configure(
            type(self).__name__, value, app_root=self._app_root)
        return

    @Option
    def logging_level(self):
        """ **Syntax:** logging_level=[CRITICAL|ERROR|WARNING|INFO|DEBUG|NOTSET]

        **Description:** Sets the threshold for the logger of this command
        invocation. Logging messages less severe than `logging_level` will be
        ignored.

        """
        return getLevelName(self.logger.getEffectiveLevel())

    @logging_level.setter
    def logging_level(self, value):
        if value is None:
            value = self._default_logging_level
        if type(value) is str:
            try:
                level = _levelNames[value.upper()]
            except KeyError:
                raise ValueError('Unrecognized logging level: %s' % value)
        else:
            try:
                level = int(value)
            except ValueError:
                raise ValueError('Unrecognized logging level: %s' % value)
        self.logger.setLevel(level)
        return

    show_configuration = Option(doc='''
        **Syntax:** show_configuration=<bool>

        **Description:** When `true`, reports command configuration in the
        messages header for this command invocation. Defaults to `false`.

        ''', default=False, validate=Boolean())

    # endregion

    # region Properties

    @property
    def configuration(self):
        """ Returns the configuration settings for this command.

        """
        return self._configuration

    @property
    def fieldnames(self):
        """ Returns the fieldnames specified as argument to this command.

        """
        return self._fieldnames

    @fieldnames.setter
    def fieldnames(self, value):
        self._fieldnames = value

    @property
    def options(self):
        """ Returns the options specified as argument to this command.

        """
        if self._option_view is None:
            self._option_view = Option.View(self)
        return self._option_view

    @property
    def search_results_info(self):
        """ Returns the search results info for this command invocation or None.

        The search results info object is created from the search results info
        file associated with the command invocation. Splunk does not pass the
        location of this file by default. You must request it by specifying
        these configuration settings in commands.conf:

        .. code-block:: python
            enableheader=true
            requires_srinfo=true

        The :code:`enableheader` setting is :code:`true` by default. Hence, you
        need not set it. The :code:`requires_srinfo` setting is false by
        default. Hence, you must set it.

        :return: :class:`SearchResultsInfo`, if :code:`enableheader` and
            :code:`requires_srinfo` are both :code:`true`. Otherwise, if either
            :code:`enableheader` or :code:`requires_srinfo` are :code:`false`,
            a value of :code:`None` is returned.

        """
        if self._search_results_info is not None:
            return self._search_results_info

        try:
            info_path = self.input_header['infoPath']
        except KeyError:
            return None

        def convert_field(field):
            return (field[1:] if field[0] == '_' else field).replace('.', '_')

        def convert_value(field, value):

            if field == 'countMap':
                split = value.split(';')
                value = dict((key, int(value)) for key, value in zip(split[0::2], split[1::2]))
            elif field == 'vix_families':
                value = ElementTree.fromstring(value)
            elif value == '':
                value = None
            else:
                try:
                    value = float(value)
                    if value.is_integer():
                        value = int(value)
                except ValueError:
                    pass

            return value

        with open(info_path, 'rb') as f:
            from collections import namedtuple
            import csv
            reader = csv.reader(f, dialect='splunklib.searchcommands')
            fields = [convert_field(x) for x in reader.next()]
            values = [convert_value(f, v) for f, v in zip(fields, reader.next())]

        search_results_info_type = namedtuple('SearchResultsInfo', fields)
        self._search_results_info = search_results_info_type._make(values)

        return self._search_results_info

    @property
    def service(self):
        """ Returns a Splunk service object for this command invocation or None.

        The service object is created from the Splunkd URI and authentication
        token passed to the command invocation in the search results info file.
        This data is not passed to a command invocation by default. You must
        request it by specifying this pair of configuration settings in
        commands.conf:

           .. code-block:: python
               enableheader=true
               requires_srinfo=true

        The :code:`enableheader` setting is :code:`true` by default. Hence, you
        need not set it. The :code:`requires_srinfo` setting is false by
        default. Hence, you must set it.

        :return: :class:`splunklib.client.Service`, if :code:`enableheader` and
            :code:`requires_srinfo` are both :code:`true`. Otherwise, if either
            :code:`enableheader` or :code:`requires_srinfo` are :code:`false`,
            a value of :code:`None` is returned.

        """
        if self._service is not None:
            return self._service

        info = self.search_results_info

        if info is None:
            return None

        splunkd = urlsplit(info.splunkd_uri, info.splunkd_protocol, allow_fragments=False)

        self._service = Service(
            scheme=splunkd.scheme, host=splunkd.hostname, port=splunkd.port, token=info.auth_token, app=info.ppc_app)

        return self._service

    # endregion

    # region Methods

    def error_exit(self, error, message=None):
        self.write_error(error.message.capitalize() if message is None else message)
        self.logger.error('Abnormal exit: %s', error)
        exit(1)

    def process(self, args=argv, input_stream=stdin, output_stream=stdout):
        """ Processes records on the `input stream optionally writing records to the output stream

        :param args:
        :param input_stream:
        :param output_stream:
        :return: :const:`None`

        """
        try:
            # getInfo exchange

            metadata, body = self._read_chunk(input_stream)
            self.fieldnames = []
            self.options.reset()
            self._message_count = 0L

            if 'args' in metadata and type(metadata['args']) == list:
                for arg in metadata['args']:
                    result = arg.split('=', 1)
                    if len(result) == 1:
                        self.fieldnames.append(result[0])
                    else:
                        name, value = result
                        try:
                            option = self.options[name]
                        except KeyError:
                            self.write_error('Unrecognized option: {0}'.format(result))
                            continue
                        try:
                            option.value = value
                        except ValueError:
                            self.write_error('Illegal value: {0}'.format(option))
                            continue
                    pass
                pass

            missing = self.options.get_missing()

            if missing is not None:
                if len(missing) == 1:
                    self.write_error('A value for "{0}" is required'.format(missing[0]))
                else:
                    self.write_error('Values for these options are required: {0}'.format(', '.join(missing)))

            if self._message_count > 0:
                # TODO: Attempt to fail gracefully as per protocol
                exit(1)

            self._configuration = type(self).ConfigurationSettings(self)
            self._write_chunk(output_stream, self._configuration.items(), '')
            output_stream.write('\n')

            if self.show_configuration:
                self.write_info('{0} command configuration settings: {1}'.format(self.name, self._configuration))

            while True:
                result = self._read_chunk(input_stream)

                if not result:
                    break

                metadata, body = result
                output_buffer = StringIO()
                input_buffer = StringIO(body)

                # TODO: Develop a lighter-weight alternative to splunk_csv
                # TODO: Ensure support for writing fields in order
                # TODO: Ensure support for multi-valued fields

                reader = csv.reader(input_buffer, dialect='splunklib.searchcommands')
                writer = csv.writer(output_buffer, dialect='splunklib.searchcommands')

                self._execute(self.stream, reader, writer)
                self._write_chunk(output_stream, metadata, output_buffer.getvalue())
                pass

        except SystemExit:
            raise

        except:

            import traceback
            import sys

            error_type, error_message, error_traceback = sys.exc_info()
            self.logger.error(traceback.format_exc(error_traceback))

            origin = error_traceback

            while origin.tb_next is not None:
                origin = origin.tb_next

            filename = origin.tb_frame.f_code.co_filename
            lineno = origin.tb_lineno

            self.write_error('%s at "%s", line %d : %s', error_type.__name__, filename, lineno, error_message)
            exit(1)

        return

    def old_process(self, args=argv, input_file=stdin, output_file=stdout):
        """ Processes search results as specified by command arguments.

        :param args: Sequence of command arguments
        :param input_file: Pipeline input file
        :param output_file: Pipeline output file

        """
        self.logger.debug('%s arguments: %s', type(self).__name__, args)
        self._configuration = None
        self._output_file = output_file

        try:
            if len(args) >= 2 and args[1] == '__GETINFO__':

                ConfigurationSettings, operation, args, reader = self._prepare(args, input_file=None)
                self.parser.parse(args, self)
                self._configuration = ConfigurationSettings(self)
                writer = splunk_csv.DictWriter(output_file, self, self.configuration.keys(), mv_delimiter=',')
                writer.writerow(self.configuration.items())

            elif len(args) >= 2 and args[1] == '__EXECUTE__':

                self.input_header.read(input_file)
                ConfigurationSettings, operation, args, reader = self._prepare(args, input_file)
                self.parser.parse(args, self)
                self._configuration = ConfigurationSettings(self)

                if self.show_configuration:
                    self.messages.append(
                        'info_message', '%s command configuration settings: %s'
                        % (self.name, self._configuration))

                writer = splunk_csv.DictWriter(output_file, self)
                self._execute(operation, reader, writer)

            else:

                file_name = path.basename(args[0])
                message = (
                    'Command {0} appears to be statically configured and static '
                    'configuration is unsupported by splunklib.searchcommands. '
                    'Please ensure that default/commands.conf contains this '
                    'stanza:\n'
                    '[{0}]\n'
                    'filename = {1}\n'
                    'supports_getinfo = true\n'
                    'supports_rawargs = true\n'
                    'outputheader = true'.format(type(self).name, file_name))
                raise NotImplementedError(message)

        except SystemExit:
            raise

        except:

            import traceback
            import sys

            error_type, error_message, error_traceback = sys.exc_info()
            self.logger.error(traceback.format_exc(error_traceback))

            origin = error_traceback

            while origin.tb_next is not None:
                origin = origin.tb_next

            filename = origin.tb_frame.f_code.co_filename
            lineno = origin.tb_lineno

            self.write_error('%s at "%s", line %d : %s', error_type.__name__, filename, lineno, error_message)
            exit(1)

        return

    @staticmethod
    def records(reader):
        keys = reader.next()
        record_count = 0L
        for record in reader:
            record_count += 1L
            yield OrderedDict(izip(keys, record))
        return

    # TODO: DVPL-5865 - Is it possible to support anything other than write_error? It does not seem so.

    def write_debug(self, message, *args):
        self._write_message('DEBUG', message, *args)
        return

    def write_error(self, message, *args):
        self._write_message('ERROR', message, *args)
        return

    def write_fatal(self, message, *args):
        self._write_message('FATAL', message, *args)
        return

    def write_info(self, message, *args):
        self._write_message('INFO', message, *args)
        return

    def write_warning(self, message, *args):
        self._write_message('WARN', message, *args)

    def _execute(self, operation, reader, writer):
        raise NotImplementedError('SearchCommand._configure(self, argv)')

    def _prepare(self, argv, input_file):
        raise NotImplementedError('SearchCommand._configure(self, argv)')

    def _write_message(self, message_type, message_text, *args):
        message_text = message_text.format(args)
        self._configuration.inspector['message.{0}.{1}'.format(self._message_count, message_type)] = message_text

    @staticmethod
    def _read_chunk(f):
        try:
            header = f.readline()
        except:
            return None

        if not header or len(header) == 0:
            return None

        m = re.match('chunked\s+1.0\s*,\s*(?P<metadata_length>\d+)\s*,\s*(?P<body_length>\d+)\s*\n', header)
        if m is None:
            print('Failed to parse transport header: {0}'.format(header), file=sys.stderr)
            return None

        try:
            metadata_length = int(m.group('metadata_length'))
            body_length = int(m.group('body_length'))
        except:
            print('Failed to parse metadata or body length', file=sys.stderr)
            return None

        print('READING CHUNK {0} {1}'.format(metadata_length, body_length), file=sys.stderr)

        try:
            metadata_buffer = f.read(metadata_length)
            body = f.read(body_length)
        except Exception as error:
            print('Failed to read metadata or body: {0}'.format(error), file=sys.stderr)
            return None

        try:
            metadata = json.loads(metadata_buffer)
        except:
            print('Failed to parse metadata JSON', file=sys.stderr)
            return None

        return [metadata, body]

    @staticmethod
    def _write_chunk(f, metadata, body):
        metadata_buffer = None
        if metadata:
            metadata_buffer = json.dumps(metadata)
        f.write('chunked 1.0,%d,%d\n' % (len(metadata_buffer) if metadata_buffer else 0, len(body)))
        f.write(metadata_buffer)
        f.write(body)
        f.flush()

    # endregion

    # region Types

    class ConfigurationSettings(object):
        """ Represents the configuration settings common to all :class:`SearchCommand` classes.

        """

        def __init__(self, command):
            self.command = command
            self._finished = False

        def __str__(self):
            """ Converts the value of this instance to its string representation.

            The value of this ConfigurationSettings instance is represented as a
            string of newline-separated :code:`name=value` pairs.

            :return: String representation of this instance

            """
            text = ', '.join(['%s=%s' % (k, getattr(self, k)) for k in self.keys()])
            return text

        # region Properties

        # Constant configuration settings

        @property
        def finished(self):
            """ Signals that the search command is complete.

            Default: :const:`True`

            """
            return self._finished

        @property
        def partial(self):
            """ Specifies whether the search command returns its response in multiple chunks.

            There are two use-cases for this:

            1. The result is very big and we'd like to emit partial chunks that fit in memory.
            2. The field set changes dramatically and we don't want to emit a sparse record set.

            If partial is :const:`True`, this chunk is part of a multi-part response. If partial is :const:`False`, this
            chunk completes a multi-part response.

            An alternative to this metadata field is to include a chunk identifier in each record. The complication of
            using a chunk identifier is that splunkd won't know that a chunk is complete until it sees a chunk with a
            new chunk identifier. The upside to having the partial field is that the common case of 1-to-1 chunks
            requires nothing from the protocol.

            Default: :const:`False`

            """
            return type(self)._partial

        _partial = False

        @property
        def stderr_dest(self):
            """ Tells Splunk what to do with messages logged to `stderr`.

            Specify one of these string values:

            ================== ========================================================
            Value              Meaning
            ================== ========================================================
            :code:`'log'`      Write messages to the job's search.log file.
            :code:`'none'`     Discard all messages logged to stderr.
            ================== ========================================================

            Default: :code:`'log'`

            """
            return type(self)._stderr_dest

        _stderr_dest = 'log'

        # endregion

        # region Methods

        @classmethod
        def configuration_settings(cls):
            """ Represents this class as a dictionary of :class:`property` instances and :code:`backing_field` names
            keyed by configuration setting name.

            This method is used by the :class:`ConfigurationSettingsType` meta-class to construct new
            :class:`ConfigurationSettings` classes.

            """
            if not hasattr(cls, '_settings'):
                is_property = lambda x: isinstance(x, property)
                cls._settings = {}
                for name, prop in getmembers(cls, is_property):
                    backing_field = '_' + name
                    if not hasattr(cls, backing_field):
                        backing_field = None
                    cls._settings[name] = (prop, backing_field)
                cls._keys = sorted(cls._settings.iterkeys())
            return cls._settings

        @classmethod
        def fix_up(cls, command_class):
            """ Adjusts and checks this class and its search command class.

            Derived classes must override this method. It is used by the
            :decorator:`Configuration` decorator to fix up the
            :class:`SearchCommand` classes it adorns. This method is overridden
            by :class:`GeneratingCommand`, :class:`ReportingCommand`, and
            :class:`StreamingCommand`, the base types for all other search
            commands.

            :param command_class: Command class targeted by this class

            """
            raise NotImplementedError('SearchCommand.fix_up method must be overridden')

        def items(self):
            """ Represents this instance as an :class:`OrderedDict`.

            This method is used by the SearchCommand.process method to report configuration settings to splunkd during
            the :code:`getInfo` exchange of the request to process search results.

            :return: :class:`OrderedDict` containing setting values keyed by name.

            """
            return OrderedDict(imap(lambda key: (key, getattr(self, key)), self.keys()))

        def keys(self):
            """ Gets the names of the settings represented by this instance.

            :return: Sorted list of setting names.

            """
            return type(self)._keys

        # endregion

    # endregion

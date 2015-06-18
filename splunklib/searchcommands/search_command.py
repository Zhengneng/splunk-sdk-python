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

# Absolute imports

from splunklib.client import Service

from collections import namedtuple, OrderedDict
from cStringIO import StringIO
from itertools import ifilter, imap, islice, izip
from logging import _levelNames, getLevelName, getLogger
from shutil import make_archive
from time import time
from urllib import unquote
from urlparse import urlsplit
from warnings import warn
from xml.etree import ElementTree

import os
import sys
import re
import csv
import tempfile
import traceback


# Relative imports

from .internals import (
    configure_logging,
    CommandLineParser,
    CsvDialect,
    InputHeader,
    Message,
    MetadataDecoder,
    MetadataEncoder,
    ObjectView,
    Recorder,
    RecordWriterV1,
    RecordWriterV2)

from . import globals
from .decorators import Option
from .validators import Boolean

# ----------------------------------------------------------------------------------------------------------------------

# P1 [ ] TODO: Log these issues against ChunkedExternProcessor
#
# 1. Implement requires_preop configuration setting.
#    This configuration setting is currently rejected by ChunkedExternProcessor.
#
# 2. Rename type=events as type=eventing for symmetry with type=reporting and type=streaming.
#    Eventing commands process records on the events pipeline.
#    This change effects ChunkedExternProcessor.cpp, eventing_command.py, and generating_command.py.

# P1 [X] TODO: Is this an Ember bug? specifying filename in commands.conf disables requires_srinfo. If you specify
# enableheader=true, requires_srinfo=true and also specify filename=generatehello.py, you do not get infoPath in the
# input_header.
# Resolved: You do not get search results info until the __EXECUTE__ action is invoked.

# ----------------------------------------------------------------------------------------------------------------------

# P1 [ ] TODO: Construct SearchCommand.metadata from SearchCommand.input_header when SearchCommand.protocol_version == 1
#
# Goals
# -----
# * SearchCommand.metadata becomes an alternative to SearchCommand.input_header that's usable under SCP v1 or SCP v2
# * A single implementation of SearchCommand.search_results_info and SearchCommand.service
#
# Requirements
# ------------
# In short, start by mapping these nine input_header values:
#
#   'allowStream' = <boolean>
#   'infoPath' = <string>
#   'keywords' = <string>
#   'preview' = <boolean>
#   'realtime' = <boolean>
#   'search' = <string>
#   'sid' = <string>
#   'splunkVersion' = <string>
#   'truncated' = <boolean>
#
# to these metadata elements:
#
# metadata = <ObjectView>
#
#   action = {'getinfo'|'execute'}  # validate=Set('getinfo', 'execute'), get=None
#   preview = <boolean>             # validate=Boolean(), get=lambda: self.input_header.get('preview')
#   searchinfo = {ObjectView}
#       app = <string>              # convert=None, get=lambda: None
#       args = <list>               # validate=List(unicode), get=TODO: get from CommandParser
#       dispatch_dir = <string>     # validate=None, get=lambda: dirname(self.input_header.get('infoPath'))
#       earliest_time = <time>      # validate=Time(), get=TODO: get from srinfo file (?)
#       latest_time = <time>        # validate=Time(), get=TODO: get from srinfo file (?)
#       owner = <string>            # validate=None, get=TODO: get from srinfo file (?)
#       raw_args = <list>           # validate=None, get=TODO: get from command line
#       search = <string>           # validate=TODO: url decode '%7C%20generatehello%20count%3D10', get=lambda: self.input_header.get('search')
#       session_key = <string>      # validate=None, get=TODO: get from srinfo file (?)
#       sid = <string>              # validate=None, get=lambda: self.input_header.get('sid')
#       splunk_version = <string>   # validate=None, get=lambda: self.input_header.get('splunkVersion')
#       splunkd_uri = <string>      # validate=None, get=lambda: TODO: get from srinfo file (?)
#       username = <string>         # validate=None, get=lambda: TODO: get from srinfo file (?)

# P1 [ ] TODO: RecordWriter.mv_delimiter to support protocol_v1
# writer = splunk_csv.DictWriter(output_file, self, self.configuration.keys(), mv_delimiter=',')

# P1 [ ] TODO: Rename globals.py because while it's allowed as a module name, it's unsatisfying that globals is also the
# name of a python builtin function. Note that global.py is not permitted as a module name because it conflicts with the
# global keyword.

# P1 [ ] TODO:Ensure that when type == 'streaming' and distributed is True we serialize type='stateful'

# P1 [ ]  TODO: Ensure that when type == 'eventing' we serialize type='events'

# P1 [ ] TODO: Phase option should print in a more user-friendly way

# P1 [ ] TODO: Verify that ChunkedExternProcessor complains if a streaming_preop has a type other than 'streaming'
# It once looked like sending type='reporting' for the streaming_preop was accepted.

# P1 [ ] TODO: Configure external_search_command for protocol version 1 (see default/commands.conf.scpv1)

# P1 [ ] TODO: Complete default/searchbnf.conf

# ----------------------------------------------------------------------------------------------------------------------

# P2 [ ] TODO: In SearchCommand.__init__ change app_root parameter to app_file because app_file is required and
# app_root can be computed from it. See globals.py and SearchCommand.__init__.

# P2 [ ] TODO: Add protocol_v1 support for recording

# P2 [ ] TODO: Write boundary tests on RecordWriter.flush

# P2 [ ] TODO: Use saved dispatch dir to mock tests that depend on its contents (?)
# To make records more generally useful to application developers we should provide/demonstrate how to mock
# self.metadata, self.search_results_info, and self.service. Such mocks might be based on archived dispatch directories.

# P2 [ ] TODO: Review and update code docs to reflect usage under protocol_version == 1 as well as protocol_version == 2

# ----------------------------------------------------------------------------------------------------------------------
# Done

# P1 [X] TODO: Validate class-level settings provided by the @Configuration decorator
# At present we have property setters that validate instance-level configuration, but we do not do any validation on
# the class-level configuration settings that are provided by way of the @Configuration decorator

# P1 [X] TODO: Save contents of dispatch dir for use in tests that may require it
# ISSUE: Some bits of data expire or change. Examples:
#   self.metadata.searchinfo.session_key
#   self.metadata.searchinfo.sid
#   self.metadata.searchinfo.splunk_uri
#   self.metadata.searchinfo.splunk_version


class SearchCommand(object):
    """ Represents a custom search command.

    """

    def __init__(self, app_root=None):
        """
        :param app_root: The root of the application directory, used primarily by tests.
        :type app_root: unicode or NoneType

        """

        # Variables that may be used, but not altered by derived classes

        class_name = self.__class__.__name__

        if app_root is None:
            self._logger, self._logging_configuration = getLogger(class_name), globals.logging_configuration
        else:
            self._logger, self._logging_configuration = configure_logging(class_name, app_root)
            globals.splunklib_logger = getLogger('splunklib')
            globals.logging_configuration = self._logging_configuration

        self._splunk_home = os.environ.get('SPLUNK_HOME')

        if self._splunk_home is None:
            globals.splunklib_logger.warning(
                'SPLUNK_HOME environment variable is undefined.\n'
                'If you are testing outside of Splunk, consider running under control of the Splunk CLI:\n'
                '    splunk cmd python %s\n'
                'If you are running inside of Splunk, SPLUNK_HOME should be defined. Consider troubleshooting your '
                'installation.', globals.app_file)
            self._splunk_home = os.getcwdu()

        # Variables backing option/property values

        self._app_root = globals.app_root if app_root is None else app_root
        self._configuration = self.ConfigurationSettings(self)
        self._input_header = InputHeader()
        self._fieldnames = None
        self._finished = None
        self._metadata = None
        self._options = None
        self._protocol_version = None
        self._search_results_info = None
        self._service = None

        # Internal variables

        self._default_logging_level = self.logger.level
        self._record_writer = None
        self._write_record = None

    def __str__(self):
        text = type(self).name + ' ' + str(self.options) + ' ' + ' '.join(self.fieldnames)
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
        self._logger, self._logging_configuration = configure_logging(self.__class__.__name__, self._app_root, value)
        globals.splunklib_logger = getLogger('splunklib')
        globals.logging_configuration = self._logging_configuration

    @Option
    def logging_level(self):
        """ **Syntax:** logging_level=[CRITICAL|ERROR|WARNING|INFO|DEBUG|NOTSET]

        **Description:** Sets the threshold for the logger of this command invocation. Logging messages less severe than
        `logging_level` will be ignored.

        """
        return getLevelName(self._logger.getEffectiveLevel())

    @logging_level.setter
    def logging_level(self, value):
        if value is None:
            value = self._default_logging_level
        if isinstance(value, (bytes, unicode)):
            try:
                level = _levelNames[value.upper()]
            except KeyError:
                raise ValueError('Unrecognized logging level: {}'.format(value))
        else:
            try:
                level = int(value)
            except ValueError:
                raise ValueError('Unrecognized logging level: {}'.format(value))
        self._logger.setLevel(level)

    record = Option(doc='''
        **Syntax: record=<bool>

        **Description:** When `true`, records the interaction between the command and splunkd. Defaults to `false`.

        ''', default=False, validate=Boolean())

    show_configuration = Option(doc='''
        **Syntax:** show_configuration=<bool>

        **Description:** When `true`, reports command configuration as an informational message. Defaults to `false`.

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
    def input_header(self):
        """ Returns the input header for this command.

        :return: The input header for this command.
        :rtype: InputHeader

        """
        warn(
            'SearchCommand.input_header is deprecated and will be removed in a future release. '
            'Please use SearchCommand.metadata instead.', DeprecationWarning, stack_level=2)
        return self._input_header

    @property
    def logger(self):
        """ Returns the logger for this command.

        :return: The logger for this command.
        :rtype:

        """
        return self._logger

    @property
    def metadata(self):
        return self._metadata

    @property
    def options(self):
        """ Returns the options specified as argument to this command.

        """
        if self._options is None:
            self._options = Option.View(self)
        return self._options

    @property
    def protocol_version(self):
        return self._protocol_version

    @property
    def search_results_info(self):
        """ Returns the search results info for this command invocation.

        The search results info object is created from the search results info file associated with the command
        invocation.

        :return: Search results info:const:`None`, if the search results info file associated with the command
        invocation is inaccessible.
        :rtype: SearchResultsInfo or NoneType

        """
        if self._search_results_info is not None:
            return self._search_results_info

        if self._protocol_version == 1:
            try:
                path = self._input_header['infoPath']
            except KeyError:
                return None
        else:
            assert self._protocol_version == 2

            try:
                dispatch_dir = self._metadata.searchinfo.dispatch_dir
            except AttributeError:
                return None

            path = os.path.join(dispatch_dir, 'info.csv')

        with open(path, 'rb') as f:
            reader = csv.reader(f, dialect=CsvDialect)
            fields = reader.next()
            values = reader.next()

        def convert_field(field):
            return (field[1:] if field[0] == '_' else field).replace('.', '_')

        decode = MetadataDecoder().decode

        def convert_value(value):
            try:
                return decode(value) if len(value) > 0 else value
            except ValueError:
                return value

        info = ObjectView(dict(imap(lambda (f, v): (convert_field(f), convert_value(v)), izip(fields, values))))

        try:
            count_map = info.countMap
        except AttributeError:
            pass
        else:
            count_map = count_map.split(';')
            n = len(count_map)
            info.countMap = dict(izip(islice(count_map, 0, n, 2), islice(count_map, 1, n, 2)))

        try:
            msg_type = info.msgType
            msg_text = info.msg
        except AttributeError:
            pass
        else:
            messages = ifilter(lambda (t, m): t or m, izip(msg_type.split('\n'), msg_text.split('\n')))
            info.msg = [Message(message) for message in messages]
            del info.msgType

        try:
            info.vix_families = ElementTree.fromstring(info.vix_families)
        except AttributeError:
            pass

        self._search_results_info = info
        return info

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

        metadata = self._metadata

        if metadata is None:
            return None

        try:
            searchinfo = self._metadata.searchinfo
        except AttributeError:
            return None

        uri = urlsplit(searchinfo.splunkd_uri, allow_fragments=False)

        self._service = Service(
            scheme=uri.scheme, host=uri.hostname, port=uri.port, app=searchinfo.app, token=searchinfo.session_key)

        return self._service

    @property
    def splunk_home(self):
        return self._splunk_home

    # endregion

    # region Methods

    def error_exit(self, error, message=None):
        self.write_error(error.message if message is None else message)
        self.logger.error('Abnormal exit: %s', error)
        exit(1)

    def finish(self):
        """ Flushes the output buffer and signals that this command has finished processing data.

        :return: :const:`None`

        """
        self._record_writer.flush(finished=True)

    def flush(self):
        """ Flushes the output buffer.

        :return: :const:`None`

        """
        self._record_writer.flush(partial=True)

    def prepare(self):
        """ Prepare for execution.

        This method should be overridden in search command classes that wish to examine and update their configuration
        or option settings prior to execution. It is called during the getinfo exchange before command metadata is sent
        to splunkd.

        :return: :const:`None`
        :rtype: NoneType

        """
        pass

    def process(self, argv=sys.argv, ifile=sys.stdin, ofile=sys.stdout):
        """ Process data.

        :param argv: Command line arguments.
        :type argv: list or tuple

        :param ifile: Input data file.
        :type ifile: file

        :param ofile: Output data file.
        :type ofile: file

        :return: :const:`None`
        :rtype: NoneType

        """
        if len(argv) > 1:
            self._process_protocol_v1(argv, ifile, ofile)
        else:
            self._process_protocol_v2(ifile, ofile)

    def _map_input_header(self):
        metadata = self._metadata
        info = metadata.searchinfo
        self._input_header.update(
            allowStream=None,
            infoPath=os.path.join(info.dispatch_dir, 'info.csv'),
            keywords=None,
            preview=metadata.preview,
            realtime=metadata.earliest_time != 0 and metadata.latest_time != 0,
            search=info.search,
            sid=info.sid,
            splunkVersion=info.splunk_version,
            truncated=None)

    def _map_metadata(self, argv):
        source = SearchCommand._MetadataSource(argv, self._input_header, self.search_results_info)

        def _map(metadata_map):
            metadata = {}

            for name, value in metadata_map.iteritems():
                if isinstance(value, dict):
                    value = _map(value)
                else:
                    transform, extract = value
                    if extract is None:
                        value = None
                    else:
                        value = extract(source)
                        if not (value is None or transform is None):
                            value = transform(value)
                metadata[name] = value

            return ObjectView(metadata)

        self._metadata = _map(SearchCommand._metadata_map)

    _metadata_map = {
        'action':
            (lambda v: 'getinfo' if v == '__GETINFO__' else 'execute' if v == '__EXECUTE__' else None, lambda s: s.argv[1]),
        'preview':
            (bool, lambda s: s.input_header.get('preview')),
        'searchinfo': {
            'app':
                (lambda v: v.ppc_app, lambda s: s.search_results_info),
            'args':
                (None, lambda s: s.argv),
            'dispatch_dir':
                (os.path.dirname, lambda s: s.input_header.get('infoPath')),
            'earliest_time':
                (lambda v: float(v.rt_earliest) if len(v.rt_earliest) > 0 else 0.0, lambda s: s.search_results_info),
            'latest_time':
                (lambda v: float(v.rt_latest) if len(v.rt_latest) > 0 else 0.0, lambda s: s.search_results_info),
            'owner':
                (None, None),
            'raw_args':
                (None, lambda s: s.argv),
            'search':
                (unquote, lambda s: s.input_header.get('search')),
            'session_key':
                (lambda v: v.auth_token, lambda s: s.search_results_info),
            'sid':
                (None, lambda s: s.input_header.get('sid')),
            'splunk_version':
                (None, lambda s: s.input_header.get('splunkVersion')),
            'splunkd_uri':
                (lambda v: v.splunkd_uri, lambda s: s.search_results_info),
            'username':
                (lambda v: v.ppc_user, lambda s: s.search_results_info)}}

    _MetadataSource = namedtuple(b'Source', (b'argv', b'input_header', b'search_results_info'))

    def _prepare_protocol_v1(self, argv, ifile, ofile):

        debug = globals.splunklib_logger.debug

        # Provide as much context as possible in advance of parsing the command line and preparing for execution

        self._record_writer = RecordWriterV1(ofile)
        self._input_header.read(ifile)
        self._protocol_version = 1
        self._map_metadata(argv)

        debug('  metadata=%r, input_header=%r', self._metadata, self._input_header)

        try:
            tempfile.tempdir = self._metadata.searchinfo.dispatch_dir
        except AttributeError:
            raise RuntimeError('%s.metadata.searchinfo.dispatch_dir is undefined'.format(self.__class__.__name__))

        debug('  tempfile.tempdir=%r', tempfile.tempdir)

        CommandLineParser.parse(self, argv[2:])
        self.prepare()

        if self.show_configuration:
            message = self.name + ' command configuration settings: ' + str(self._configuration)
            self._record_writer.write_message('info_message', message)

        self._write_record = self._record_writer.write_record

    def _process_protocol_v1(self, argv, ifile, ofile):
        debug = globals.splunklib_logger.debug
        class_name = self.__class__.__name__

        debug('%s.process started under protocol_version=1', class_name)
        # noinspection PyBroadException
        try:
            if argv[1] == '__GETINFO__':

                debug('Writing configuration settings')

                self._prepare_protocol_v1(argv, ifile, ofile)
                record = self._configuration.items()
                self._write_record(record)
                self.finish()

            elif argv[1] == '__EXECUTE__':

                debug('Executing')

                self._prepare_protocol_v1(argv, ifile, ofile)
                self._execute(ifile, None)

            else:
                message = (
                    'Command {0} appears to be statically configured for search command protocol version 1 and static '
                    'configuration is unsupported by splunklib.searchcommands. Please ensure that '
                    'default/commands.conf contains this stanza:\n'
                    '[{0}]\n'
                    'filename = {1}\n'
                    'supports_getinfo = true\n'
                    'supports_rawargs = true\n'
                    'outputheader = true'.format(self.name, os.path.basename(argv[0])))
                raise RuntimeError(message)

        except SystemExit:
            self.flush()
            raise

        except:
            self._report_unexpected_error()
            self.flush()
            exit(1)

        debug('%s.process finished under protocol_version=1', class_name)

    def _process_protocol_v2(self, ifile, ofile):
        """ Processes records on the `input stream optionally writing records to the output stream.

        :param ifile: Input file object.
        :type ifile: file or InputType

        :param ofile: Output file object.
        :type ofile: file or OutputType

        :return: :const:`None`

        """
        self._protocol_version = 2
        debug = globals.splunklib_logger.debug
        class_name = self.__class__.__name__
        debug('%s.process started under protocol_version=2', class_name)

        # Read search command metadata from splunkd
        # noinspection PyBroadException
        try:
            debug('Reading metadata')
            metadata, body = self._read_chunk(ifile)

            action = getattr(metadata, 'action', None)

            if action != 'getinfo':
                raise RuntimeError('Expected getinfo action, not {}'.format(action))

            if len(body) > 0:
                raise RuntimeError('Did not expect data for getinfo action')

            self._metadata = metadata
            self._map_input_header()

            debug('  metadata=%r, input_header=%r', self._metadata, self._input_header)

            try:
                tempfile.tempdir = self._metadata.searchinfo.dispatch_dir
            except AttributeError:
                raise RuntimeError('%s.metadata.searchinfo.dispatch_dir is undefined'.format(class_name))

            debug('  tempfile.tempdir=%r', tempfile.tempdir)
        except:
            self._record_writer = RecordWriterV2(ofile)
            self._report_unexpected_error()
            self._record_writer.flush(finished=True)
            exit(1)

        # Write search command configuration for consumption by splunkd
        # noinspection PyBroadException
        try:
            self._record_writer = RecordWriterV2(ofile, getattr(self._metadata, 'maxresultrows', None))
            self.fieldnames = []
            self.options.reset()

            args = self.metadata.searchinfo.args
            error_count = 0

            debug('Parsing arguments')

            if args and type(args) == list:
                for arg in args:
                    result = arg.split('=', 1)
                    if len(result) == 1:
                        self.fieldnames.append(result[0])
                    else:
                        name, value = result
                        try:
                            option = self.options[name]
                        except KeyError:
                            self.write_error('Unrecognized option: {}={}'.format(name, value))
                            error_count += 1
                            continue
                        try:
                            option.value = value
                        except ValueError:
                            self.write_error('Illegal value: {}'.format(option))
                            error_count += 1
                            continue
                    pass
                pass

            missing = self.options.get_missing()

            if missing is not None:
                if len(missing) == 1:
                    self.write_error('A value for "{}" is required'.format(missing[0]))
                else:
                    self.write_error('Values for these required options are missing: {}'.format(', '.join(missing)))
                error_count += 1

            self._configuration = self._new_configuration_settings()  # included in the output even when error_count > 0

            if error_count > 0:
                exit(1)

            debug('  command=%s', unicode(self))

            debug('Preparing for execution')
            self.prepare()

            if self.record:

                # Create the recordings directory, if it doesn't already exist

                recordings = os.path.join(self._splunk_home, 'var', 'run', 'splunklib.searchcommands', 'recordings')

                if not os.path.isdir(recordings):
                    os.makedirs(recordings)

                # Create input/output recorders from ifile and ofile

                recording = os.path.join(recordings, class_name + '-' + repr(time()))
                ifile = Recorder(recording + '.input', ifile)
                self._record_writer.ofile = Recorder(recording + '.output', ofile)

                # Record the metadata that initiated this command after removing the record option from args/raw_args

                info = self._metadata.searchinfo

                for attr in 'args', 'raw_args':
                    setattr(info, attr, [arg for arg in getattr(info, attr) if not arg.startswith('record=')])

                metadata = MetadataEncoder().encode(self._metadata)
                ifile.record('chunked 1.0,' + unicode(len(metadata)) + ',0\n' + metadata)

                # Archive the dispatch dir because it is useful for developing tests (use it as a baseline in mocks)
                root_dir, base_dir = os.path.split(info.dispatch_dir)
                make_archive(recording + '.dispatch_dir', 'gztar', root_dir, base_dir, logger=self.logger)

            if self.show_configuration:
                self.write_info('{} command configuration settings: {}'.format(self.name, self.configuration))

            debug('  configuration=%s', self._configuration)
            self._record_writer.write_metadata(self._configuration)

        except SystemExit:
            self._record_writer.write_metadata(self._configuration)
            raise
        except:
            self._report_unexpected_error()
            self._record_writer.write_metadata(self._configuration)
            exit(1)

        # Execute search command on data passing through the pipeline
        # noinspection PyBroadException
        try:
            debug('Executing under protocol_version=2')
            self._execute(ifile, None)
        except SystemExit:
            self.flush()
            raise
        except:
            self._report_unexpected_error()
            self.flush()
            exit(1)

        debug('%s.process completed', class_name)

    def write_debug(self, message, *args):
        self._record_writer.write_message('DEBUG', message, *args)

    def write_error(self, message, *args):
        self._record_writer.write_message('ERROR', message, *args)

    def write_fatal(self, message, *args):
        self._record_writer.write_message('FATAL', message, *args)

    def write_info(self, message, *args):
        self._record_writer.write_message('INFO', message, *args)

    def write_warning(self, message, *args):
        self._record_writer.write_message('WARN', message, *args)

    def write_metric(self, name, value):
        """ Writes a metric that will be added to the search inspector.

        :param name: Name of the metric.
        :type name: basestring

        :param value: A 4-tuple containing the value of metric :param:`name` where

            value[0] = Elapsed seconds or :const:`None`.
            value[1] = Number of invocations or :const:`None`.
            value[2] = Input count or :const:`None`.
            value[3] = Output count or :const:`None`.

        The :data:`SearchMetric` type provides a convenient encapsulation of :param:`value`.
        The :data:`SearchMetric` type provides a convenient encapsulation of :param:`value`.

        :return: :const:`None`.

        """
        self._record_writer.write_metric(name, value)

    # P2 [ ] TODO: Support custom inspector values

    @staticmethod
    def _decode_list(mv):
        return [match.replace('$$', '$') for match in SearchCommand._encoded_value.findall(mv)]

    _encoded_value = re.compile(r'\$(?P<item>(?:\$\$|[^$])*)\$(?:;|$)')  # matches a single value in an encoded list

    def _execute(self, ifile, process):
        """ Default processing loop

        :param ifile: Input file object.
        :type ifile: file

        :param process: Bound method to call in processing loop.
        :type process: instancemethod

        :return: :const:`None`.
        :rtype: NoneType

        """
        write_record = self._write_record
        for record in process(self._records(ifile)):
            write_record(record)
        self.finish()

    def _new_configuration_settings(self):
        return self.ConfigurationSettings(self)

    @staticmethod
    def _read_chunk(ifile):

        # noinspection PyBroadException
        try:
            header = ifile.readline()
        except Exception as error:
            raise RuntimeError('Failed to read transport header: {}'.format(error))

        if not header:
            return None

        match = SearchCommand._header.match(header)

        if match is None:
            raise RuntimeError('Failed to parse transport header: {}'.format(header))

        metadata_length, body_length = match.groups()
        metadata_length = int(metadata_length)
        body_length = int(body_length)

        try:
            metadata = ifile.read(metadata_length)
        except Exception as error:
            raise RuntimeError('Failed to read metadata of length {}: {}'.format(metadata_length, error))

        decoder = MetadataDecoder()

        try:
            metadata = decoder.decode(metadata)
        except Exception as error:
            raise RuntimeError('Failed to parse metadata of length {}: {}'.format(metadata_length, error))

        try:
            body = ifile.read(body_length)
        except Exception as error:
            raise RuntimeError('Failed to read body of length {}: {}'.format(body_length, error))

        return metadata, body

    _header = re.compile(r'chunked\s+1.0\s*,\s*(\d+)\s*,\s*(\d+)\s*\n')

    def _records(self, ifile):

        finished = None

        while not finished:
            result = self._read_chunk(ifile)

            if not result:
                return

            metadata, body = result

            action = getattr(metadata, 'action', None)

            if action != 'execute':
                raise RuntimeError('Expected execute action, not {}'.format(action))

            reader = csv.reader(StringIO(body), dialect=CsvDialect)
            finished = getattr(metadata, 'finished', False)

            try:
                fieldnames = reader.next()
            except StopIteration:
                return

            # P2 [ ] TODO: Consider string interning (see intern built-in) for some performance improvement in this
            # loop should performance data back this approach

            mv_fieldnames = {name: name[len('__mv_'):] for name in fieldnames if name.startswith('__mv_')}

            if len(mv_fieldnames) == 0:
                for values in reader:
                    yield OrderedDict(izip(fieldnames, values))
                continue

            for values in reader:
                record = OrderedDict()
                for fieldname, value in izip(fieldnames, values):
                    if fieldname.startswith('__mv_'):
                        if len(value) > 0:
                            record[mv_fieldnames[fieldname]] = self._decode_list(value)
                    elif fieldname not in record:
                        record[fieldname] = value
                yield record

                # raise RuntimeError('Expected splunkd to terminate command on receipt of finished signal')

    def _report_unexpected_error(self):

        error_type, error, tb = sys.exc_info()
        origin = tb

        while origin.tb_next is not None:
            origin = origin.tb_next

        filename = origin.tb_frame.f_code.co_filename
        lineno = origin.tb_lineno
        message = '{0} at "{1}", line {2:d} : {3}'.format(error_type.__name__, filename, lineno, error)

        globals.splunklib_logger.error(message + '\nTraceback:\n' + ''.join(traceback.format_tb(tb)))
        self.write_error(message)

    # endregion

    # region Types

    class ConfigurationSettings(object):
        """ Represents the configuration settings common to all :class:`SearchCommand` classes.

        """

        def __init__(self, command):
            self.command = command

        def __repr__(self):
            """ Converts the value of this instance to its string representation.

            The value of this ConfigurationSettings instance is represented as a string of comma-separated
            :code:`(name, value)` pairs.

            :return: String representation of this instance

            """
            definitions = type(self).configuration_setting_definitions
            settings = imap(
                lambda setting: repr((setting.name, setting.__get__(self), setting.supporting_protocols)), definitions)
            return '[' + ', '.join(settings) + ']'

        def __str__(self):
            """ Converts the value of this instance to its string representation.

            The value of this ConfigurationSettings instance is represented as a string of comma-separated
            :code:`name=value` pairs. Items with values of :const:`None` are filtered from the list.

            :return: String representation of this instance

            """
            items = imap(lambda (name, value): name + '=' + repr(value), self.iteritems())
            return ', '.join(items)

        # region Methods

        @classmethod
        def fix_up(cls, command_class):
            """ Adjusts and checks this class and its search command class.

            Derived classes must override this method. It is used by the :decorator:`Configuration` decorator to fix up
            the :class:`SearchCommand` class it adorns. This method is overridden by :class:`EventingCommand`,
            :class:`GeneratingCommand`, :class:`ReportingCommand`, and :class:`StreamingCommand`, the base types for
            all other search commands.

            :param command_class: Command class targeted by this class

            """
            raise NotImplementedError('SearchCommand.Configuration.fix_up method must be overridden')

        def items(self):
            """ Represents this instance as an :class:`OrderedDict`.

            This method is used by :meth:`SearchCommand.process` to werite configuration settings to splunkd during
            the :code:`getInfo` or :code:`__GETINFO__` exchange of the request to process search results. Only items
            with values that have been set are rendered.

            :return: :class:`dict` containing setting values keyed by name.

            """
            return OrderedDict(self.iteritems())

        def iteritems(self):
            definitions = type(self).configuration_setting_definitions
            version = self.command.protocol_version
            return ifilter(
                lambda (name, value): value is not None, imap(
                    lambda setting: (setting.name, setting.__get__(self)), ifilter(
                        lambda setting: setting.is_supported_by_protocol(version), definitions)))

            # endregion

            # endregion


def dispatch(command_class, argv=sys.argv, input_file=sys.stdin, output_file=sys.stdout, module_name=None):
    """ Instantiates and executes a search command class

    This function implements a `conditional script stanza <http://goo.gl/OFaox6>`_ based on the value of
    :code:`module_name`::

        if module_name is None or module_name == '__main__':
            # execute command

    Call this function at module scope with :code:`module_name=__name__`, if you would like your module to act as either
    a reusable module or a standalone program. Otherwise, if you wish this function to unconditionally instantiate and
    execute :code:`command_class`, pass :const:`None` as the value of :code:`module_name`.

    :param command_class: Search command class to instantiate and execute.
    :type command_class: type
    :param argv: List of arguments to the command.
    :type argv: list or tuple
    :param input_file: File from which the command will read data.
    :type input_file: :code:`file`
    :param output_file: File to which the command will write data.
    :type output_file: :code:`file`
    :param module_name: Name of the module calling :code:`dispatch` or :const:`None`.
    :type module_name: :code:`basestring`
    :returns: :const:`None`

    **Example**

    .. code-block:: python
        :linenos:

        #!/usr/bin/env python
        from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
        @Configuration()
        class SomeStreamingCommand(StreamingCommand):
            ...
            def stream(records):
                ...
        dispatch(SomeStreamingCommand, module_name=__name__)

    Dispatches the :code:`SomeStreamingCommand`, if and only if
    :code:`__name__` is equal to :code:`'__main__'`.

    **Example**

    .. code-block:: python
        :linenos:

        from splunklib.searchcommands import dispatch, StreamingCommand, Configuration, Option, validators
        @Configuration()
        class SomeStreamingCommand(StreamingCommand):
            ...
            def stream(records):
                ...
        dispatch(SomeStreamingCommand)

    Unconditionally dispatches :code:`SomeStreamingCommand`.

    """
    assert issubclass(command_class, SearchCommand)

    if module_name is None or module_name == '__main__':
        command_class().process(argv, input_file, output_file)

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

from collections import deque, namedtuple, OrderedDict
from cStringIO import StringIO
from itertools import chain, ifilter, imap
from json import JSONDecoder, JSONEncoder
from logging import getLogger, root, StreamHandler
from logging.config import fileConfig
from numbers import Number
from urllib import unquote

import csv
import io
import os
import re
import sys

csv.field_size_limit(10485760)  # The default value is 128KB; upping to 10MB. See SPL-12117 for background on this issue

if sys.platform == 'win32':
    # Work around the fact that on Windows '\n' is mapped to '\r\n'. The typical solution is to simply open files in
    # binary mode, but stdout is already open, thus this hack. 'CPython' and 'PyPy' work differently. We assume that
    # all other Python implementations are compatible with 'CPython'. This might or might not be a valid assumption.
    from platform import python_implementation
    implementation = python_implementation()
    fileno = sys.stdout.fileno()
    if implementation == 'PyPy':
        sys.stdout = os.fdopen(fileno, 'wb', 0)
    else:
        from msvcrt import setmode
        setmode(fileno, os.O_BINARY)

_global = sys.modules['splunklib.searchcommands.globals']  # Get access globals without creating a circular reference

_splunk_home = os.path.realpath(os.path.join(os.getcwdu(), os.environ.get('SPLUNK_HOME', '')))
_current_logging_configuration_file = None


def configure_logging(name, app_root, path=None):
    """ Configure logging and return the named logger and the location of the logging configuration file loaded.

    This function expects a Splunk app directory structure::

        <app-root>
            bin
                ...
            default
                ...
            local
                ...

    This function looks for a logging configuration file at each of these locations, loading the first, if any,
    logging configuration file that it finds::

        local/{name}.logging.conf
        default/{name}.logging.conf
        local/logging.conf
        default/logging.conf

    The current working directory is set to *<app-root>* before the logging configuration file is loaded. Hence, paths
    in the logging configuration file are relative to *<app-root>*. The current directory is reset before return.

    You may short circuit the search for a logging configuration file by providing an alternative file location in
    `path`. Logging configuration files must be in `ConfigParser format`_.

    #Arguments:

    :param name: Logger name
    :type name: bytes, unicode

    :param app_root: The root of the application directory.
    :type app_root: bytes or unicode

    :param path: Location of an alternative logging configuration file or `None`.
    :type path: bytes, unicode or NoneType

    :returns: The named logger and the location of the logging configuration file loaded.
    :rtype: tuple

    .. _ConfigParser format: http://goo.gl/K6edZ8

    """
    if path is None:
        if name is None:
            probing_paths = [os.path.join('local', 'logging.conf'), os.path.join('default', 'logging.conf')]
        else:
            probing_paths = [
                os.path.join('local', name + '.logging.conf'),
                os.path.join('default', name + '.logging.conf'),
                os.path.join('local', 'logging.conf'),
                os.path.join('default', 'logging.conf')]
        for relative_path in probing_paths:
            configuration_file = os.path.join(app_root, relative_path)
            if os.path.exists(configuration_file):
                path = configuration_file
                break
    elif not os.path.isabs(path):
        found = False
        for conf in 'local', 'default':
            configuration_file = os.path.join(app_root, conf, path)
            if os.path.exists(configuration_file):
                path = configuration_file
                found = True
                break
        if not found:
            raise ValueError(
                'Logging configuration file "{}" not found in local or default directory'.format(path))
    elif not os.path.exists(path):
        raise ValueError('Logging configuration file "{}" not found'.format(path))

    if path is not None:
        path = os.path.realpath(path)

    global _current_logging_configuration_file

    if path != _current_logging_configuration_file:
        working_directory = os.getcwdu()
        os.chdir(app_root)
        try:
            # P1 [ ] TODO: Ensure that all existing loggers are still usable after loading a logging configuration file
            fileConfig(path, {'SPLUNK_HOME': _splunk_home})
        finally:
            os.chdir(working_directory)
        _current_logging_configuration_file = path

    if len(root.handlers) == 0:
        root.addHandler(StreamHandler())

    return None if name is None else getLogger(name), path


class CommandLineParser(object):
    """ Parses the arguments to a search command.

    A search command line is described by the following syntax.

    **Syntax**::

       command       = command-name *[wsp option] *[wsp [dquote] field-name [dquote]]
       command-name  = alpha *( alpha / digit )
       option        = option-name [wsp] "=" [wsp] option-value
       option-name   = alpha *( alpha / digit / "_" )
       option-value  = word / quoted-string
       word          = 1*( %01-%08 / %0B / %0C / %0E-1F / %21 / %23-%FF ) ; Any character but DQUOTE and WSP
       quoted-string = dquote *( word / wsp / "\" dquote / dquote dquote ) dquote
       field-name    = ( "_" / alpha ) *( alpha / digit / "_" / "." / "-" )

    **Note:**

    This syntax is constrained to an 8-bit character set.

    **Note:**

    This syntax does not show that `field-name` values may be comma-separated
    when in fact they can be. This is because Splunk strips commas from the
    command line. A custom search command will never see them.

    **Example:**
    countmatches fieldname = word_count pattern = \w+ some_text_field

    Option names are mapped to properties in the targeted ``SearchCommand``. It
    is the responsibility of the property setters to validate the values they
    receive. Property setters may also produce side effects. For example,
    setting the built-in `log_level` immediately changes the `log_level`.

    """
    @classmethod
    def parse(cls, command, argv):
        """ Splits an argument list into an options dictionary and a fieldname
        list.

        The argument list, `argv`, must be of the form::

            *[option]... *[<field-name>]

        Options are validated and assigned to items in `command.options`. Field
        names are validated and stored in the list of `command.fieldnames`.

        #Arguments:

        :param command: Search command instance.
        :type command: ``SearchCommand``
        :param argv: List of search command arguments.
        :type argv: ``list``
        :return: ``None``

        #Exceptions:

        ``SyntaxError``: Argument list is incorrectly formed.
        ``ValueError``: Unrecognized option/field name, or an illegal field value.

        """
        # Prepare

        # P1 [ ] TODO: Switch to globals.splunklib_logger

        command.logger.debug('Parsing %s command line: %s', type(command).__name__, repr(argv))

        command_args = ' '.join(argv)
        command.fieldnames = None
        command.options.reset()

        command_args = cls._arguments_re.match(command_args)

        if command_args is None:
            raise SyntaxError("Syntax error: {}".format(command_args))

        # Parse options

        for option in cls._options_re.finditer(command_args.group('options')):
            name, value = option.group(1), option.group(2)
            if name not in command.options:
                raise ValueError('Unrecognized option: {}={}'.format(name, repr(value)))
            command.options[name].value = cls.unquote(value)

        missing = command.options.get_missing()

        if missing is not None:
            if len(missing) == 1:
                raise ValueError('A value for "{}" is required'.format(missing[0]))
            raise ValueError('Values for these options are required: {}'.format(', '.join(missing)))

        # Parse field names

        command.fieldnames = command_args.group('fieldnames').split()

        command.logger.debug('    %s: %s', type(command).__name__, command)

    @classmethod
    def unquote(cls, string):
        """ Removes quotes from a quoted string.

        Splunk search command quote rules are applied. The enclosing double-quotes, if present, are removed. Escaped
        double-quotes ('\"' or '""') are replaced by a single double-quote ('"').

        **NOTE**

        We are not using a json.JSONDecoder because Splunk quote rules are different than JSON quote rules. A
        json.JSONDecoder does not recognize a pair of double-quotes ('""') as an escaped quote ('"') and will
        decode single-quoted strings ("'") in addition to double-quoted ('"') strings.

        """
        if len(string) == 0:
            return ''

        if len(string) == 1:
            return string

        if string[0] == '"':
            if string[-1] != '"':
                raise ValueError("Poorly formed string literal: " + string)
            string = string[1:-1]

        def replace(match):
            value = match.group(0)
            if value == '""':
                return '"'
            if len(value) != 2:
                raise ValueError("Poorly formed string literal: " + string)
            return value[1]

        result = re.sub(cls._escaped_quote_re, replace, string)
        return result

    # region Class variables

    _arguments_re = re.compile(r"""
        ^\s*
        (?P<options>    # Match a leading set of name/value pairs
            (?:
                (?:[_a-zA-Z][_a-zA-Z0-9]+)          # name
                \s*=\s*                             # =
                (?:[^\s"]+|"(?:[^"]+|""|\\")*")\s*? # value
            )*
        )
        \s*
        (?P<fieldnames> # Match a trailing set of field names
            (?:(?:[_a-zA-Z][_.a-zA-Z0-9-]+|"[_a-zA-Z][_.a-zA-Z0-9-]+")\s*)*
        )
        \s*$
        """, re.VERBOSE)

    _escaped_quote_re = re.compile(r'(\\.|""|[\\"])')

    _name_re = re.compile(r"""[_a-zA-Z][[_a-zA-Z0-9]+""")

    _options_re = re.compile(r"""
        # Captures a set of name/value pairs when used with re.finditer
        ([_a-zA-Z][_a-zA-Z0-9]+)         # name
        \s*=\s*                          # =
        ([^\s"]+|"(?:[^\\"]+|\\.|"")*")  # value
        """, re.VERBOSE)

    # endregion


class ConfigurationSettingsType(type):
    """ Metaclass for constructing ConfigurationSettings classes.

    Instances of :class:`ConfigurationSettingsType` construct :class:`ConfigurationSettings` classes from classes from
    a base :class:`ConfigurationSettings` class and a dictionary of configuration settings. The settings in the
    dictionary are validated against the settings in the base class. You cannot add settings, you can only change their
    backing-field values and you cannot modify settings without backing-field values. These are considered fixed
    configuration setting values.

    This is an internal class used in two places:

    + :meth:`decorators.Configuration.__call__`

      Adds a ConfigurationSettings attribute to a :class:`SearchCommand` class.

    + :meth:`reporting_command.ReportingCommand.fix_up`

      Adds a ConfigurationSettings attribute to a :meth:`ReportingCommand.map` method, if there is one.

    """
    def __new__(mcs, module, name, bases):
        mcs = super(ConfigurationSettingsType, mcs).__new__(mcs, name, bases, {})
        return mcs

    def __init__(cls, module, name, bases):

        super(ConfigurationSettingsType, cls).__init__(name, bases, None)
        cls.__module__ = module

    @staticmethod
    def validate_configuration_setting(specification, name, value):
        if not isinstance(value, specification.type):
            if isinstance(specification.type, type):
                type_names = specification.type.__name__
            else:
                type_names = ', '.join(imap(lambda t: t.__name__, specification.type))
            raise ValueError('Expected {} value, not {}={}'.format(type_names, name, repr(value)))
        if specification.constraint and not specification.constraint(value):
            raise ValueError('Illegal value: {}={}'.format(name, repr(value)))
        return value

    specification = namedtuple(
        b'ConfigurationSettingSpecification', (
            b'type',
            b'constraint',
            b'supporting_protocols'))

    # P1 [ ] TODO: Review ConfigurationSettingsType.specification_matrix for completeness and correctness

    specification_matrix = {
        'clear_required_fields': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'distributed': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[2]),
        'generates_timeorder': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'generating': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'maxinputs': specification(
            type=int,
            constraint=lambda value: 0 <= value <= sys.maxsize,
            supporting_protocols=[2]),
        'overrides_timeorder': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'required_fields': specification(
            type=(list, set, tuple),
            constraint=None,
            supporting_protocols=[1, 2]),
        'requires_preop': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1, 2]),
        'retainsevents': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'run_in_preview': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1, 2]),
        'streaming': specification(
            type=bool,
            constraint=None,
            supporting_protocols=[1]),
        'streaming_preop': specification(
            type=(bytes, unicode),
            constraint=None,
            supporting_protocols=[1, 2]),
        'type': specification(
            type=(bytes, unicode),
            constraint=lambda value: value in ('eventing', 'reporting', 'streaming'),
            supporting_protocols=[2])}


class CsvDialect(csv.Dialect):
    """ Describes the properties of Splunk CSV streams """
    delimiter = b','
    quotechar = b'"'
    doublequote = True
    skipinitialspace = False
    lineterminator = b'\r\n'
    quoting = csv.QUOTE_MINIMAL


class InputHeader(dict):
    """ Represents a Splunk input header as a collection of name/value pairs.

    """
    def read(self, ifile):
        """ Reads an input header from an input file.

        The input header is read as a sequence of *<name>***:***<value>* pairs separated by a newline. The end of the
        input header is signalled by an empty line or an end-of-file.

        :param ifile: File-like object that supports iteration over lines.

        """
        name, value = None, None

        for line in ifile:
            if line == '\n':
                break
            item = line.split(':', 1)
            if len(item) == 2:
                # start of a new item
                if name is not None:
                    self[name] = value[:-1]  # value sans trailing newline
                name, value = item[0], unquote(item[1])
            elif name is not None:
                # continuation of the current item
                value += unquote(line)

        if name is not None: self[name] = value[:-1] if value[-1] == '\n' else value


Message = namedtuple(b'Message', (b'type', b'text'))


class MetadataDecoder(JSONDecoder):

    def __init__(self):
        JSONDecoder.__init__(self, object_hook=self._object_hook)

    @staticmethod
    def _object_hook(dictionary):

        object_view = ObjectView(dictionary)
        stack = deque()
        stack.append((None, None, dictionary))

        while len(stack):
            instance, member_name, dictionary = stack.popleft()

            for name, value in dictionary.iteritems():
                if isinstance(value, dict):
                    stack.append((dictionary, name, value))

            if instance is not None:
                instance[member_name] = ObjectView(dictionary)

        return object_view


class MetadataEncoder(JSONEncoder):

    def __init__(self):
        JSONEncoder.__init__(self, separators=self._separators)

    def default(self, o):
        return o.__dict__ if isinstance(o, ObjectView) else JSONEncoder.default(self, o)

    _separators = (',', ':')


class ObjectView(object):

    def __init__(self, dictionary):
        self.__dict__ = dictionary

    def __repr__(self):
        return repr(self.__dict__)

    def __str__(self):
        return str(self.__dict__)


class Recorder(object):

    def __init__(self, path, f):
        self._recording = io.open(path, 'wb')
        self._file = f

    # P2 [ ] TODO: Implement __dir__ because we delegate to self._file (?)

    def __getattr__(self, name):
        return getattr(self._file, name)

    def read(self, size=None):
        value = self._file.read() if size is None else self._file.read(size)
        self._recording.write(value)
        self._recording.flush()
        return value

    def readline(self, size=None):
        value = self._file.readline() if size is None else self._file.readline(size)
        if len(value) > 0:
            self._recording.write(value)
            self._recording.flush()
        return value

    def record(self, text):
        self._recording.write(text)

    def write(self, text):
        self._recording.write(text)
        self._file.write(text)
        self._recording.flush()


class RecordWriter(object):

    def __init__(self, ofile, maxresultrows=None):
        self._maxresultrows = 50000 if maxresultrows is None else maxresultrows

        self._ofile = ofile
        self._fieldnames = None
        self._buffer = StringIO()

        self._writer = csv.writer(self._buffer, dialect=CsvDialect)
        self._writerow = self._writer.writerow
        self._finished = False

        self._inspector = OrderedDict()
        self._chunk_count = 0
        self._record_count = 0
        self._total_record_count = 0L

    @property
    def ofile(self):
        return self._ofile

    @ofile.setter
    def ofile(self, value):
        self._ofile = value

    def flush(self, finished=None, partial=None):
        assert finished is None or isinstance(finished, bool)
        assert partial is None or isinstance(partial, bool)
        assert finished is None or partial is None
        self._ensure_validity()

    def write_message(self, message_type, message_text, *args, **kwargs):
        self._ensure_validity()
        self._inspector.setdefault('messages', []).append((message_type, message_text.format(*args, **kwargs)))

    def write_record(self, record):
        self._ensure_validity()

        if self._fieldnames is None:
            fieldnames = imap(lambda key: unicode(key).encode('utf-8'), record.iterkeys())
            fieldnames = imap(lambda fieldname: (fieldname, b'__mv_' + fieldname), fieldnames)
            fieldnames = list(chain.from_iterable(fieldnames))
            self._writer.writerow(fieldnames)
            self._fieldnames = record.keys()

        values = imap(lambda fieldname: self._encode_value(record.get(fieldname, None)), self._fieldnames)
        values = list(chain.from_iterable(values))
        self._writerow(values)
        self._record_count += 1

        if self._record_count >= self._maxresultrows:
            self.flush(partial=True)

    def _clear(self):
        self._buffer.reset()
        self._buffer.truncate()
        self._inspector.clear()
        self._record_count = 0

    def _encode_value(self, value):

        def to_string(item):

            if item is None:
                return b''
            if isinstance(item, bool):
                return b't' if item else b'f'
            if isinstance(item, bytes):
                return item
            if isinstance(item, Number):
                return str(item)

            if not isinstance(item, unicode):
                item = self._encode_dict(item) if isinstance(item, dict) else repr(item)

            return item.encode('utf-8', errors='backslashreplace')

        if not isinstance(value, (list, tuple)):
            return to_string(value), None

        if len(value) == 0:
            return None, None

        if len(value) == 1:
            return to_string(value[0]), None

        # P1 [ ] TODO: If a list item contains newlines, its single value cannot be interpreted correctly
        # Question: Must we return a value? Is it good enough to return (None, <encoded-list>)?
        # See what other splunk commands do.

        value = imap(lambda item: to_string(item), value)
        sv, mv = reduce(lambda (s, m), v: (s + v + b'\n', m + v.replace(b'$', b'$$') + b'$;$'), value, (b'', b'$'))

        return sv[:-1], mv[:-2]

    def _ensure_validity(self):
        if self._finished is True:
            assert self._record_count == 0 and len(self._inspector) == 0
            raise RuntimeError('I/O operation on closed record writer')

    _encode_dict = JSONEncoder(separators=(',', ':')).encode


class RecordWriterV1(RecordWriter):

    def flush(self, finished=None, partial=None):

        RecordWriter.flush(self, finished=None, partial=None)

        if self._record_count > 0 or (self._chunk_count == 0 and 'messages' in self._inspector):

            write = self._ofile.write

            if self._chunk_count == 0:

                messages = self._inspector.get('messages')

                if messages:

                    for level, text in messages:
                        write(RecordWriterV1._message_level[level])
                        write('=')
                        write(text)
                        write('\r\n')

                write('\r\n')

            write(self._buffer.getvalue())
            self._clear()
            self._chunk_count += 1
            self._total_record_count += self._record_count

        self._finished = finished is True

    _message_level = {
        'DEBUG': 'debug_message',
        'ERROR': 'error_message',
        'FATAL': 'error_message',
        'INFO': 'info_message',
        'WARN': 'warn_message'
    }

class RecordWriterV2(RecordWriter):

    def flush(self, finished=None, partial=None):

        RecordWriter.flush(self, finished=None, partial=None)

        if self._record_count > 0 or len(self._inspector) > 0:

            # P2 [ ] TODO: Write SearchMetric (?) Include timing (?) Anything else (?)

            self._total_record_count += self._record_count
            self._chunk_count += 1

            metadata = {
                'inspector': self._inspector if len(self._inspector) else None,
                'finished': finished,
                'partial': partial}

            self._write_chunk(metadata, self._buffer.getvalue())
            self._clear()

        self._finished = finished is True

    def write_metadata(self, configuration):
        self._ensure_validity()
        metadata = OrderedDict(chain(
            configuration.items(), (('inspector', self._inspector if self._inspector else None),)))
        self._write_chunk(metadata, '')
        self._ofile.write('\n')
        self._clear()

    def write_metric(self, name, value):
        self._ensure_validity()
        self._inspector['metric.' + name] = value

    def _clear(self):
        RecordWriter._clear(self)
        self._fieldnames = None

    def _write_chunk(self, metadata, body):

        if metadata:
            metadata = OrderedDict(ifilter(lambda x: x[1] is not None, metadata.iteritems()))
            metadata = self._encode_metadata(metadata)
            metadata_length = len(metadata)
        else:
            metadata_length = 0

        body_length = len(body)

        if not (metadata_length > 0 or body_length > 0):
            return

        start_line = 'chunked 1.0,' + unicode(metadata_length) + ',' + unicode(body_length) + '\n'
        write = self._ofile.write
        write(start_line)
        write(metadata)
        write(body)
        self._ofile.flush()

    _encode_metadata = MetadataEncoder().encode

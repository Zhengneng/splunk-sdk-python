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
from . search_command import SearchCommand
from . internals import CsvDialect
from cStringIO import StringIO
from itertools import chain, ifilterfalse, imap

import csv


# TODO: Edit EventingCommand class documentation

class EventingCommand(SearchCommand):
    """ Applies a transformation to search results as they travel through the events pipeline.

    Eventing commands typically filter, sort, modify, or combine search
    results. Splunk will send search results in batches of up to 50,000 records.
    Hence, a search command must be prepared to be invoked many times during the
    course of pipeline processing. Each invocation should produce a set of
    results independently usable by downstream processors.

    By default Splunk may choose to run a streaming command locally on a search
    head and/or remotely on one or more indexers concurrently. The size and
    frequency of the search result batches sent to the command will vary based
    on scheduling considerations. Streaming commands are typically invoked many
    times during the course of pipeline processing.

    You can tell Splunk to run your streaming command locally on a search head,
    never remotely on indexers.

    .. code-block:: python

        @Configuration(local=False)
        class SomeStreamingCommand(StreamingCommand):
            ...

    If your streaming command modifies the time order of event records you must
    tell Splunk to ensure correct behavior.

    .. code-block:: python

        @Configuration(overrides_timeorder=True)
        class SomeStreamingCommand(StreamingCommand):
            ...

    :ivar input_header: :class:`InputHeader`:  Collection representing the input
        header associated with this command invocation.

    :ivar messages: :class:`MessagesHeader`: Collection representing the output
        messages header associated with this command invocation.

    """
    # region Methods

    def stream(self, records):
        """ Generator function that processes and yields records to the Splunk events pipeline.

        You must override this method.

        """
        raise NotImplementedError('StreamingCommand.stream(self, records)')

    def _execute(self, ifile, ofile):
        """ Execution loop

        :param ifile: Input file object.
        :type ifile: file

        :param ofile: Output file object.
        :type ofile: file

        :return: `None`.

        """
        while True:
            result = self._read_chunk(ifile)

            if not result:
                break

            # TODO: understand all metadata received and store any metadata that's useful to a command
            metadata, body = result
            input_buffer = StringIO(body)
            reader = csv.reader(input_buffer, dialect=CsvDialect)
            writer = csv.writer(self._output_buffer, dialect=CsvDialect)

            record_count = 0L
            keys = None

            for record in self.stream(self._records(reader)):
                if keys is None:
                    keys = [chain.from_iterable(imap(lambda key: (key, '__mv_' + key), record))]
                    writer.writerow(keys)
                values = [chain.from_iterable(
                    imap(lambda value: self._encode_value(value), imap(lambda key: record[key], record)))]
                writer.writerow(values)
                record_count += 1L
                if self.partial:
                    self._write_records(ofile)

            self._write_records(ofile)

    # endregion

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """ Represents the configuration settings that apply to a :class:`StreamingCommand`.

        """
        # region Properties

        @property
        def required_fields(self):
            """ List of required fields for this search (back-propagates to the generating search).

            Setting this value enables selected fields mode.

            Default: :const:`['*']`

            """
            return getattr(self, '_required_fields', type(self)._required_fields)

        @required_fields.setter
        def required_fields(self, value):
            if not (value is None or isinstance(value, (list, tuple))):
                raise ValueError('Expected a list or tuple of field names or None, not {0}.'.format(repr(value)))
            setattr(self, '_required_fields', value)

        _required_fields = None

        @property
        def type(self):
            """ Command type

            Computed: :const:`'streaming'`, if :code:`distributed` is :const:`False`; otherwise, if :code:`generating`
            is :const:`True`, :const:`'stateful'`.

            """
            return 'eventing'

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """ Verifies :code:`command` class structure.

            """
            if command.stream == EventingCommand.stream:
                raise AttributeError('No EventingCommand.stream override')
            return

        # endregion

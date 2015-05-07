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

from .search_command import SearchCommand
from .internals import CsvDialect

from cStringIO import StringIO
from itertools import chain, ifilterfalse, imap

import csv

# TODO: Edit StreamingCommand class documentation


class StreamingCommand(SearchCommand):
    """ Applies a transformation to search results as they travel through the stream pipeline.

    Streaming commands typically filter, augment, or update, search result records. Splunk will send them in batches of
    up to 50,000 records. Hence, a search command must be prepared to be invoked many times during the course of
    pipeline processing. Each invocation should produce a set of results independently usable by downstream processors.

    By default Splunk may choose to run a streaming command locally on a search head and/or remotely on one or more
    indexers concurrently. The size and frequency of the search result batches sent to the command will vary based
    on scheduling considerations.

    You can tell Splunk to run your streaming command locally on a search head, never remotely on indexers.

    .. code-block:: python

        @Configuration(distributed=False)
        class CentralizedStreamingCommand(StreamingCommand):
            ...

    """
    # region Methods

    def stream(self, records):
        """ Generator function that processes and yields event records to the Splunk stream pipeline.

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
        def distributed(self):
            """ True, if this command should be distributed to indexers

            Default: :const:`True`

            """
            return getattr(self, '_distributed', type(self)._distributed)

        @distributed.setter
        def distributed(self, value):
            if not (value is None or isinstance(value, bool)):
                raise ValueError('Expected True, False, or None, not {0}.'.format(repr(value)))
            setattr(self, '_distributed', value)

        _distributed = None

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
            return 'streaming'

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """ Verifies :code:`command` class structure.

            """
            if command.stream == StreamingCommand.stream:
                raise AttributeError('No StreamingCommand.stream override')
            return

        def render(self):

            sequence = ifilterfalse(
                lambda item: item[0] == 'distributed', super(StreamingCommand.ConfigurationSettings, self).render())

            if self.distributed:
                return sequence

            return imap(lambda item: item if item[0] != 'type' else (item[0], 'stateful'), sequence)

        # endregion

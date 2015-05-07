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
from .search_command import SearchCommand
from .internals import CsvDialect
from itertools import chain, imap, ifilterfalse

import csv


class GeneratingCommand(SearchCommand):
    """ Generates events based on command arguments.

    Generating commands receive no input and must be the first command on a pipeline. By default Splunk will run your
    command locally on a search head. By default generating commands run in the streams pipeline.

    .. code-block:: python

        @Configuration()
        class StreamingGeneratingCommand(GeneratingCommand)
            ...

    You can change the pipeline that a generating command runs in using the :code:`type` configuration setting. For
    example, to run your command in the eventing pipeline, configure your command like this:

    .. code-block:: python

        @Configuration(type=`eventing`)
        class EventingGeneratingCommand(GeneratingCommand)
            ...

        @Configuration(type=`reporting`)
        class ReportingGeneratingCommand(GeneratingCommand)
            ...

    You can tell Splunk to run your streaming generating command in a distributed manner on a search head, or remotely
    on indexers:

    .. code-block:: python

        @Configuration(distributed=True)
        class SomeGeneratingCommand(GeneratingCommand)
            ...

    Only streaming generating commands may be distributed.

    """
    # region Methods

    def generate(self):
        """ A generator that yields records to the Splunk processing pipeline

        You must override this method.

        """
        raise NotImplementedError('GeneratingCommand.generate(self)')

    def _execute(self, ifile, ofile):
        """ Execution loop

        :param ifile: Input file object. Unused.
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
            writer = csv.writer(self._output_buffer, dialect=CsvDialect)

            record_count = 0L
            keys = None

            for record in self.generate():
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

    # region Types

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """ Represents the configuration settings for a :code:`GeneratingCommand` class.

        """
        # region Properties

        @property
        def distributed(self):
            """ True, if this command should be distributed to indexers.

            This value is ignored unless :meth:`type` is equal to :const:`streaming`. It is only these command types
            that may be distributed.

            Default: :const:`False`

            """
            return getattr(self, '_distributed', type(self)._distributed)

        @distributed.setter
        def distributed(self, value):
            if not (value is None or isinstance(value, bool)):
                raise ValueError('Expected True, False, or None, not {0}.'.format(repr(value)))
            setattr(self, '_distributed', value)

        _distributed = None

        @property
        def type(self):
            """ One of the strings that represent the command type.

            ====================  ======================================================================================
            Value                 Description
            --------------------  --------------------------------------------------------------------------------------
            :const:`'eventing'`   Runs as the first command in the Splunk events pipeline. Cannot be distributed.
            :const:`'reporting'`  Runs as the first command in the Splunk reports pipeline. Cannot be distributed.
            :const:`'streaming'`  Runs as the first command in the Splunk streams pipeline. May be distributed.
            ====================  ======================================================================================

            Default: :const:`'streaming'`

            """
            return type(self)._type

        @type.setter
        def type(self, value):
            if not (isinstance(value, basestring) and value in ('eventing', 'reporting', 'streaming')):
                raise ValueError('Expected a value of "eventing", "reporting", or "streaming"; not {0}.'.format(
                    repr(value)))
            self._type = value

        _type = 'streaming'

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """ Verifies :code:`command` class structure.

            """
            if command.generate == GeneratingCommand.generate:
                raise AttributeError('No GeneratingCommand.generate override')
            return

        def render(self):

            sequence = chain(ifilterfalse(
                lambda item: item[0] == 'distributed', super(GeneratingCommand.ConfigurationSettings, self).render()),
                (('generating', True),))

            if not (self.distributed and self.type == 'streaming'):
                return sequence

            return imap(lambda item: item if item[0] != 'type' else (item[0], 'stateful'), sequence)

        # endregion

    # endregion

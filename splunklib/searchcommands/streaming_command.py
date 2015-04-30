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
from . search_command import SearchCommand
from cStringIO import StringIO

import csv


class StreamingCommand(SearchCommand):
    """ Applies a transformation to search results as they travel through the processing pipeline.

    Streaming commands typically filter, sort, modify, or combine search
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
        """ Generator function that processes and yields event records to the
        Splunk processing pipeline.

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

            metadata, body = result
            output_buffer = StringIO()
            input_buffer = StringIO(body)

            # TODO: Ensure support for multi-valued fields

            reader = csv.reader(input_buffer, dialect='splunklib.searchcommands')
            writer = csv.writer(output_buffer, dialect='splunklib.searchcommands')

            # TODO: Write metadata produced by the command, not the metadata read by the command

            record_count = 0L
            keys = None

            for record in self.stream(self._records(reader)):
                if keys is None:
                    keys = tuple(key for key in record)
                    writer.writerow(keys)
                writer.writerow(tuple(record.get(key, '') for key in keys))
                record_count += 1L

            self._write_chunk(ofile, metadata, output_buffer.getvalue())
            pass

    # endregion

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """ Represents the configuration settings that apply to a :class:`StreamingCommand`.

        """
        def __init__(self, command):
            super(StreamingCommand.ConfigurationSettings, self).__init__(command)
            self._required_fields = ('*',)

        # region Properties

        @property
        def maxinputs(self):
            """ Specifies the maximum number of events desired in each chunk of data from splunkd.

            Default: :const:`0`

            """
            return self._maxinputs

        _maxinputs = 0

        @property
        def required_fields(self):
            """ List of required fields for this search (back-propagates to the generating search).

            For streaming and stateful commands, setting this enables "selected fields mode" (defined below.)

            Default: :const:`'*'`

            """
            return self._required_fields

        @property
        def run_in_preview(self):
            """ Specifies whether to run this command when generating results for preview or wait for final output.

            This may be important for commands that have side effects (e.g. outputlookup)

            Default: :const:`True`

            """
            return type(self)._run_in_preview

        _run_in_preview = True

        @property
        def type(self):
            """ Command type

            Fixed: :const:`'streaming'`

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

        # endregion

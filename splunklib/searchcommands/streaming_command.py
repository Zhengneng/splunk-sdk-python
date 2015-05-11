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
from itertools import ifilter

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

    def _execute(self, ifile, process):
        SearchCommand._execute(self, ifile, self.stream)

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
        def maxinputs(self):
            """ Specifies the maximum number of events that can be passed to the command for each invocation.

            This limit cannot exceed the value of `maxresultrows` in `limits.conf`.

            Default: The value of maxresultrows.

            """
            return getattr(self, '_maxinputs', type(self)._maxinputs)

        @maxinputs.setter
        def maxinputs(self, value):
            setattr(self, '_maxinputs', value)

        _maxinputs = None

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

            Computed: :const:`'streaming'`, if :code:`distributed` is :const:`False`; otherwise :const:`'stateful'`.

            """
            return 'stateful' if self.distributed is False else 'streaming'

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
            return ifilter(lambda item: item[1] is not None and item[0] != 'distributed', self.iteritems())

        # endregion

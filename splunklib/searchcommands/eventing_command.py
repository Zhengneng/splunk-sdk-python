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
from . import configuration_setting


# TODO: Edit EventingCommand class documentation

class EventingCommand(SearchCommand):
    """ Applies a transformation to search results as they travel through the events pipeline.

    Eventing commands typically filter, group, order, and/or or augment event records. Examples of eventing commands
    from Splunk's built-in command set include sort_, dedup_, and cluster_. Each execution of an eventing command
    should produce a set of event records independently usable by downstream processors.

    .. _sort: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Sort
    .. _dedup: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Dedup
    .. _cluster: http://docs.splunk.com/Documentation/Splunk/latest/SearchReference/Cluster

    """
    # region Methods

    def transform(self, records):
        """ Generator function that processes and yields event records to the Splunk events pipeline.

        You must override this method.

        """
        raise NotImplementedError('EventingCommand.transform(self, records)')

    def _execute(self, ifile, process):
        SearchCommand._execute(self, ifile, self.transform)

    # endregion

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """ Represents the configuration settings that apply to a :class:`EventingCommand`.

        """
        # region Properties

        # TODO: Centralize setting documentation

        maxinputs = configuration_setting('maxinputs', doc='''
            Specifies the maximum number of events that can be passed to the command for each invocation.

            This limit cannot exceed the value of `maxresultrows` in `limits.conf`.

            Default: The value of maxresultrows.

            ''')

        required_fields = configuration_setting('required_fields', doc='''
            List of required fields for this search (back-propagates to the generating search).

            Setting this value enables selected fields mode.

            Default: :const:`['*']`

            ''')

        # TODO: Request renaming this type as 'eventing'. Eventing commands process records on the events pipeline.
        # This change effects ChunkedExternProcessor.cpp, eventing_command.py, and generating_command.py.

        type = configuration_setting('type', readonly=True, value='eventing', doc='''
            Command type

            Fixed: :const:`'eventing'`.

            ''')

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """ Verifies :code:`command` class structure.

            """
            if command.transform == EventingCommand.transform:
                raise AttributeError('No EventingCommand.transform override')
            SearchCommand.ConfigurationSettings.fix_up(command)

        # endregion

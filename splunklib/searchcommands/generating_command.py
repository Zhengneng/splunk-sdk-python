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
from . import ConfigurationSetting

from itertools import imap, ifilter


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

    def _execute(self, ifile, process):
        """ Execution loop

        :param ifile: Input file object. Unused.
        :type ifile: file

        :return: `None`.

        """
        self._record_writer.write_records(self.generate())
        self.finish()

    # endregion

    # region Types

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """ Represents the configuration settings for a :code:`GeneratingCommand` class.

        """
        # region SCP v1/v2 Properties

        generating = ConfigurationSetting(readonly=True, value=True, doc='''
            Tells Splunk that this command generates events, but does not process inputs.

            Generating commands must appear at the front of the search pipeline identified by :meth:`type`.

            Fixed: :const:`True`

            ''')

        # endregion

        # region SCP v1 Properties

        generates_timeorder = ConfigurationSetting(value=False, doc='''
            :const:`True`, if the command generates new events.

            Default: :const:`False`

            ''')

        retainsevents = ConfigurationSetting(value=False, doc='''
            :const:`True`, if the command retains events the way the sort, dedup, and cluster commands do, or whether it
            transforms them the way the stats command does.

            ''')

        streaming = ConfigurationSetting(value=True, doc='''
            :const:`True`, if the command is streamable.

            Default: :const:`True`

            ''')

        # endregion

        # region SCP v2 Properties

        distributed = ConfigurationSetting(value=False, doc='''
            True, if this command should be distributed to indexers.

            This value is ignored unless :meth:`type` is equal to :const:`streaming`. It is only this command type that
            may be distributed.

            Default: :const:`False`

            ''')

        type = ConfigurationSetting(value='streaming', doc='''
            A command type name.

            ====================  ======================================================================================
            Value                 Description
            --------------------  --------------------------------------------------------------------------------------
            :const:`'eventing'`   Runs as the first command in the Splunk events pipeline. Cannot be distributed.
            :const:`'reporting'`  Runs as the first command in the Splunk reports pipeline. Cannot be distributed.
            :const:`'streaming'`  Runs as the first command in the Splunk streams pipeline. May be distributed.
            ====================  ======================================================================================

            Default: :const:`'streaming'`

            ''')

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """ Verifies :code:`command` class structure.

            """
            if command.generate == GeneratingCommand.generate:
                raise AttributeError('No GeneratingCommand.generate override')

        def iteritems(self):
            iteritems = SearchCommand.ConfigurationSettings.iteritems(self)
            version = self.command.protocol_version
            if version == 2:
                iteritems = ifilter(lambda (name, value): name != 'distributed', iteritems)
                if self.distributed and self.type == 'streaming':
                    iteritems = imap(
                        lambda (name, value): (name, 'stateful') if name == 'type' else (name, value), iteritems)
            return iteritems

        # endregion

    # endregion

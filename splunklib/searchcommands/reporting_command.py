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

from .internals import ConfigurationSettingsType
from .streaming_command import StreamingCommand
from .search_command import SearchCommand
from . import ConfigurationSetting
from .decorators import Option

from itertools import chain

# P1 [ ] TODO: Edit ReportingCommand class documentation


class ReportingCommand(SearchCommand):
    """ Processes search result records and generates a reporting data structure.

    Reporting search commands run as either reduce or map/reduce operations. The reduce part runs on a search head and
    is responsible for processing a single chunk of search results to produce the command's reporting data structure.
    The map part is called a streaming preop. It feeds the reduce part with partial results and by default runs on the
    search head and/or one or more indexers.

    You must implement a :meth:`reduce` method as a generator function that iterates over a set of event records and
    yields a reporting data structure. You may implement a :meth:`map` method as a generator function that iterates
    over a set of event records and yields :class:`dict` or :class:`list(dict)` instances.

    **ReportingCommand configuration**

    Configure the :meth:`map` operation using a Configuration decorator on your :meth:`map` method. Configure it like
    you would a :class:`StreamingCommand`. Configure the :meth:`reduce` operation using a Configuration decorator on
    your :meth:`ReportingCommand` class.

    """
    # region Special methods

    def __init__(self, app_root=None):
        SearchCommand.__init__(self, app_root)
        self._phase = self.reduce

    # endregion

    # region Options

    @Option
    def phase(self):
        """ **Syntax:** phase=[map|reduce]

        **Description:** Identifies the phase of the current map-reduce operation.

        """
        return self._phase

    @phase.setter
    def phase(self, value):
        if value is None or value == 'reduce':
            self._phase = self.reduce
            return
        if value == 'map':
            self._phase = self.map
            return
        raise ValueError('Expected a value of "map" or "reduce", or None, not {0}'.format(repr(value)))

    # endregion

    # region Methods

    def map(self, records):
        """ Override this method to compute partial results.

        :param records:
        :type records:

        You must override this method, if :code:`requires_preop=True`.

        """
        return NotImplemented

    def reduce(self, records):
        """ Override this method to produce a reporting data structure.

        You must override this method.

        """
        raise NotImplementedError('reduce(self, records)')

    def _execute(self, ifile, process):
        SearchCommand._execute(self, ifile, self._phase)

    def _new_configuration_settings(self):
        if self._phase == self.map:
            # noinspection PyUnresolvedReferences
            return self.map.ConfigurationSettings(self)
        if self._phase == self.reduce:
            settings = self.ConfigurationSettings(self)
            settings.streaming_preop = ' '.join(chain((self.name, 'phase="map"', str(self._options)), self.fieldnames))
            return settings
        raise RuntimeError('Unrecognized reporting command phase: {}'.format(repr(self._phase)))

    # endregion

    # region Types

    class ConfigurationSettings(SearchCommand.ConfigurationSettings):
        """ Represents the configuration settings for a :code:`ReportingCommand`.

        """
        # region SCP v1 Properties

        clear_required_fields = ConfigurationSetting(doc='''
            :const:`True`, if required_fields represent the *only* fields required.

            If :const:`False`, required_fields are additive to any fields that may be required by subsequent commands.
            In most cases, :const:`True` is appropriate for reporting commands.

            Default: :const:`True`

            ''')

        # endregion

        # region SCP v2 Properties

        maxinputs = ConfigurationSetting(doc='''
            Specifies the maximum number of events that can be passed to the command for each invocation.

            This limit cannot exceed the value of `maxresultrows` in `limits.conf`.

            Default: The value of maxresultrows.

            ''')

        requires_preop = ConfigurationSetting(doc='''
            Indicates whether :meth:`ReportingCommand.map` is required for proper command execution.

            If :const:`True`, :meth:`ReportingCommand.map` is guaranteed to be called. If :const:`False`, Splunk
            considers it to be an optimization that may be skipped.

            Default: :const:`False`

            ''')

        retainsevents = ConfigurationSetting(value=False, doc='''
            Signals that :meth:`ReportingCommand.reduce` transforms _raw events to produce a reporting data structure.

            Fixed: :const:`False`

            ''')

        run_in_preview = ConfigurationSetting(doc='''
            :const:`True`, if this command should be run to generate results for preview; not wait for final output.

            This may be important for commands that have side effects (e.g. outputlookup)

            Default: :const:`True`

            ''')

        streaming = ConfigurationSetting(value=False, doc='''
            Signals that :meth:`ReportingCommand.reduce` runs on the search head.

            Fixed: :const:`False`

            ''')

        streaming_preop = ConfigurationSetting(doc='''
            Denotes the requested streaming preop search string.

            Computed.

            ''')

        type = ConfigurationSetting(readonly=True, value='reporting', doc='''
            Command type name.

            Fixed: :const:`'reporting'`.

            ''')

        # endregion

        # region Methods

        @classmethod
        def fix_up(cls, command):
            """ Verifies :code:`command` class structure and configures the :code:`command.map` method.

            Verifies that :code:`command` derives from :class:`ReportingCommand` and overrides
            :code:`ReportingCommand.reduce`. It then configures :code:`command.reduce`, if an overriding implementation
            of :code:`ReportingCommand.reduce` has been provided.

            :param command: :code:`ReportingCommand` class

            Exceptions:

            :code:`TypeError` :code:`command` class is not derived from :code:`ReportingCommand`
            :code:`AttributeError` No :code:`ReportingCommand.reduce` override

            """
            if not issubclass(command, ReportingCommand):
                raise TypeError('{} is not a ReportingCommand'.format( command))

            if command.reduce == ReportingCommand.reduce:
                raise AttributeError('No ReportingCommand.reduce override')

            if command.map == ReportingCommand.map:
                cls._requires_preop = False
                return

            f = vars(command)[b'map']   # Function backing the map method

            # EXPLANATION OF PREVIOUS STATEMENT: There is no way to add custom attributes to methods. See [Why does
            # setattr fail on a method](http://goo.gl/aiOsqh) for a discussion of this issue.

            try:
                settings = f._settings
            except AttributeError:
                f.ConfigurationSettings = StreamingCommand.ConfigurationSettings
                return

            # Create new StreamingCommand.ConfigurationSettings class

            module = command.__module__ + b'.' + command.__name__ + b'.map'
            name = b'ConfigurationSettings'
            bases = (StreamingCommand.ConfigurationSettings,)

            f.ConfigurationSettings = ConfigurationSettingsType(module, name, bases)
            ConfigurationSetting.fix_up(f.ConfigurationSettings, settings)
            del f._settings

        # endregion

    # endregion

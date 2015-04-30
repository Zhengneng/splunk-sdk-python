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


class ConfigurationSettingsType(type):
    """ Metaclass for constructing ConfigurationSettings classes.

    Instances of the ConfigurationSettingsType construct ConfigurationSettings
    classes from a base ConfigurationSettings class and a dictionary of
    configuration settings. The settings in the dictionary are validated against
    the settings in the base class. You cannot add settings, you can only
    change their backing-field values and you cannot modify settings without
    backing-field values. These are considered fixed configuration setting
    values.

    This is an internal class used in two places:

    + decorators.Configuration.__call__

      Adds a ConfigurationSettings attribute to a SearchCommand class.

    + reporting_command.ReportingCommand.fix_up

      Adds a ConfigurationSettings attribute to a ReportingCommand.map method,
      if there is one.

    """
    def __new__(mcs, module, name, bases, settings):
        mcs = super(ConfigurationSettingsType, mcs).__new__(mcs, name, bases, {})
        return mcs

    def __init__(cls, module, name, bases, settings):

        super(ConfigurationSettingsType, cls).__init__(name, bases, None)
        configuration_settings = cls.configuration_settings()

        for name, value in settings.iteritems():
            try:
                prop, backing_field = configuration_settings[name]
            except KeyError:
                raise AttributeError('{0} has no {1} configuration setting'.format(cls, name))
            if backing_field is None:
                raise AttributeError('The value of configuration setting {0} is managed'.format(name))
            setattr(cls, backing_field, value)

        cls.__module__ = module
        return

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

from logging import getLogger, root, StreamHandler
from logging.config import fileConfig
from collections import deque

import csv
import os
import sys

csv.field_size_limit(10485760)  # The default value is 128KB; upping to 10MB. See SPL-12117 for background on this issue


def configure_logging(name, probing_path=None, app_root=None):
    """ Configure logging and return a logger and the location of its logging configuration file.

    This function expects:

    + A Splunk app directory structure::

        <app-root>
            bin
                ...
            default
                ...
            local
                ...

    + The current working directory is *<app-root>***/bin**.

      Splunk guarantees this. If you are running the app outside of Splunk, be
      sure to set the current working directory to *<app-root>***/bin** before
      calling.

    This function looks for a logging configuration file at each of these
    locations, loading the first, if any, logging configuration file that it
    finds::

        local/{name}.logging.conf
        default/{name}.logging.conf
        local/logging.conf
        default/logging.conf

    The current working directory is set to *<app-root>* before the logging
    configuration file is loaded. Hence, paths in the logging configuration
    file are relative to *<app-root>*. The current directory is reset before
    return.

    You may short circuit the search for a logging configuration file by
    providing an alternative file location in `probing_path`. Logging configuration
    files must be in `ConfigParser format`_.

    #Arguments:

    :param name: Logger name
    :type name: str
    :param probing_path: Location of an alternative logging configuration file or `None`
    :type probing_path: str or NoneType
    :returns: A logger and the location of its logging configuration file
    :param app_root: The root of the application directory, used primarily by tests.
    :type app_root: str or NoneType

    .. _ConfigParser format: http://goo.gl/K6edZ8

    """
    app_directory = get_app_directory(sys.argv[0]) if app_root is None else app_root

    if probing_path is None:
        probing_paths = [
            'local/%s.logging.conf' % name,
            'default/%s.logging.conf' % name,
            'local/logging.conf',
            'default/logging.conf']
        for relative_path in probing_paths:
            configuration_file = os.path.join(app_directory, relative_path)
            if os.path.exists(configuration_file):
                probing_path = configuration_file
                break
    elif not os.path.isabs(probing_path):
        found = False
        for conf in 'local', 'default':
            configuration_file = os.path.join(app_directory, conf, probing_path)
            if os.path.exists(configuration_file):
                probing_path = configuration_file
                found = True
                break
        if not found:
            raise ValueError(
                'Logging configuration file "%s" not found in local or default '
                'directory' % probing_path)
    elif not os.path.exists(probing_path):
        raise ValueError('Logging configuration file "%s" not found')

    if probing_path is not None:
        working_directory = os.getcwd()
        os.chdir(app_directory)
        try:
            splunk_home = os.path.normpath(os.path.join(working_directory, os.environ['SPLUNK_HOME']))
        except KeyError:
            splunk_home = working_directory  # reasonable in debug scenarios
        try:
            probing_path = os.path.abspath(probing_path)
            fileConfig(probing_path, {'SPLUNK_HOME': splunk_home})
        finally:
            os.chdir(working_directory)

    if len(root.handlers) == 0:
        root.addHandler(StreamHandler())

    logger = getLogger(name)
    return logger, probing_path


def get_app_directory(path):
    return os.path.dirname(os.path.abspath(os.path.dirname(path)))


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


class CsvDialect(csv.Dialect):
    """ Describes the properties of Splunk CSV streams. """
    delimiter = b','
    quotechar = b'"'
    doublequote = True
    skipinitialspace = False
    lineterminator = b'\r\n'
    quoting = csv.QUOTE_MINIMAL


class _ObjectView(object):
    def __init__(self, dictionary):
        self.__dict__ = dictionary

    def __repr__(self):
        return repr(self.__dict__)

    def __str__(self):
        return str(self.__dict__)


class ObjectView(_ObjectView):
    def __init__(self, dictionary):
        super(ObjectView, self).__init__(dictionary)
        stack = deque()
        stack.append((None, None, dictionary))
        while len(stack):
            instance, member_name, dictionary = stack.popleft()
            for name, value in dictionary.iteritems():
                if isinstance(value, dict):
                    stack.append((dictionary, name, value))
            if instance is not None:
                instance[member_name] = _ObjectView(dictionary)
        return

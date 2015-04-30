# coding=utf-8
#
# Copyright © Splunk, Inc. All Rights Reserved

""" Sets the packages path and optionally starts the Python remote debugging client.

The Python remote debugging client depends on the settings of the variables defined in _debug_conf.py.  Set these
variables in _debug_conf.py to enable/disable debugging using either the JetBrains PyCharm or Eclipse PyDev remote
debugging packages, which must be unzipped and copied to packages/pydebug.

"""

from __future__ import absolute_import, division, print_function, unicode_literals
from collections import OrderedDict
from os import path
from sys import modules, path as sys_path, stderr

import platform


def initialize_app():

    module_dir = path.dirname(path.realpath(__file__))

    for packages in path.join(module_dir, 'packages'), path.join(path.join(module_dir, 'packages', platform.system())):
        if not path.isdir(packages):
            break
        sys_path.insert(0, path.join(packages))

    configuration_file = path.join(module_dir, '_pydebug_conf.py')

    if not path.exists(configuration_file):
        return

    remote_debugging = OrderedDict([
        ('client_package_location', path.join(module_dir, '_pydebug.egg')),
        ('is_enabled', False),
        ('host', None),
        ('port', 5678),
        ('suspend', True),
        ('stderr_to_server', False),
        ('stdout_to_server', False),
        ('overwrite_prev_trace', False),
        ('patch_multiprocessing', False),
        ('trace_only_current_thread', False)])

    execfile(configuration_file, {}, remote_debugging)

    if remote_debugging['is_enabled']:

        debug_client = remote_debugging['client_package_location']

        if path.exists(debug_client):

            host, port = remote_debugging['host'], remote_debugging['port']
            sys_path.insert(1, debug_client)
            import pydevd

            print('Connecting to Python debug server at {0}:{1}'.format(host, port), file=stderr)
            stderr.flush()

            try:
                pydevd.settrace(
                    host=host,
                    port=port,
                    suspend=remote_debugging['suspend'],
                    stderrToServer=remote_debugging['stderr_to_server'],
                    stdoutToServer=remote_debugging['stdout_to_server'],
                    overwrite_prev_trace=remote_debugging['overwrite_prev_trace'],
                    patch_multiprocessing=remote_debugging['patch_multiprocessing'],
                    trace_only_current_thread=remote_debugging['trace_only_current_thread'])
            except SystemExit as error:
                print('Failed to connect to Python debug server at {0}:{1}: {2}'.format(host, port, error), file=stderr)
                stderr.flush()
            else:
                print('Connected to Python debug server at {0}:{1}'.format(host, port), file=stderr)
                stderr.flush()

    modules[__name__].remote_debugging = remote_debugging
    return

initialize_app()

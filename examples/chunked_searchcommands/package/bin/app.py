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
from signal import signal, SIGTERM
from sys import path as sys_path, stderr

import platform
import atexit

remote_debugging = None
settrace = lambda: NotImplemented
stoptrace = settrace


def initialize():

    module_dir = path.dirname(path.realpath(__file__))
    system = platform.system()

    for packages in path.join(module_dir, 'packages'), path.join(path.join(module_dir, 'packages', system)):
        if not path.isdir(packages):
            break
        sys_path.insert(0, path.join(packages))

    configuration_file = path.join(module_dir, '_pydebug_conf.py')

    if not path.exists(configuration_file):
        return

    debug_client = path.join(module_dir, '_pydebug.egg')

    if not path.exists(debug_client):
        return

    _remote_debugging = OrderedDict([
        ('client_package_location', debug_client),
        ('is_enabled', False),
        ('host', None),
        ('port', 5678),
        ('suspend', True),
        ('stderr_to_server', False),
        ('stdout_to_server', False),
        ('overwrite_prev_trace', False),
        ('patch_multiprocessing', False),
        ('trace_only_current_thread', False)])

    execfile(configuration_file, {}, _remote_debugging)
    sys_path.insert(1, debug_client)
    import pydevd

    def _settrace():
        host, port = _remote_debugging['host'], _remote_debugging['port']

        print('Connecting to Python debug server at {0}:{1}'.format(host, port), file=stderr)
        stderr.flush()

        try:
            pydevd.settrace(
                host=host,
                port=port,
                suspend=_remote_debugging['suspend'],
                stderrToServer=_remote_debugging['stderr_to_server'],
                stdoutToServer=_remote_debugging['stdout_to_server'],
                overwrite_prev_trace=_remote_debugging['overwrite_prev_trace'],
                patch_multiprocessing=_remote_debugging['patch_multiprocessing'],
                trace_only_current_thread=_remote_debugging['trace_only_current_thread'])
        except SystemExit as error:
            print('Failed to connect to Python debug server at {0}:{1}: {2}'.format(host, port, error), file=stderr)
            stderr.flush()
        else:
            print('Connected to Python debug server at {0}:{1}'.format(host, port), file=stderr)
            stderr.flush()

    global remote_debugging
    remote_debugging = _remote_debugging

    global settrace
    settrace = _settrace

    global stoptrace
    stoptrace = pydevd.stoptrace

    if _remote_debugging['is_enabled']:
        settrace()

    if system == 'Windows':
        pass
        # try:
        #     import win32api
        #     win32api.SetConsoleCtrlHandler(func, True)
        # except ImportError:
        #     version = “.”.join(map(str, sys.version_info[:2]))
        #     raise Exception(”pywin32 not installed for Python ” + version)
    else:
        signal(SIGTERM, pydevd.stoptrace)

    atexit.register(pydevd.stoptrace)


initialize()

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
import __main__
import os
import sys

app_root = os.path.dirname(os.path.abspath(os.path.dirname(__main__.__file__)))

def execute(path, argv=None, environ=None):
    ExternalSearchCommand(path, argv, environ).execute()


class ExternalSearchCommand(object):
    """
    """
    
    def __init__(self, path=None, argv=None, environ=None):
        self._path = self._argv = self._environ = None
        if path is not None:
            self.path = path
        if argv is not None:
            self.argv = argv
        if environ is not None:
            self.environ = environ

    # region Properties

    @property
    def argv(self):
        return self._argv

    @argv.setter
    def argv(self, value):
        if not (value is None or isinstance(value, (list, tuple))):
            raise ValueError('Expected a list, tuple or value of None for argv, not {}', repr(value))
        self._argv = value

    @property
    def environ(self):
        return self._environ

    @environ.setter
    def environ(self, value):
        if not isinstance(value, dict):
            raise ValueError('Expected a dictionary value for environ, not {}', repr(value))
        self._environ = value

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        if not isinstance(value, basestring):
            raise ValueError('Expected a string value for path, not {}', repr(value))
        self._path = unicode(value)

    # endregion

    # region Methods

    def execute(self):
        try:
            if self._path is None:
                raise ValueError('A value for {}.path is required.'.format(self.__class__.__name__))

            search_path = os.getenv('Path') if self._environ is None else self._environ.get('Path')
            executable = ExternalSearchCommand._search_path(self._path, search_path)

            if executable is None:
                raise ValueError('Cannot find command "{}" on path.'.format(self._path))

            self._path = executable

            if self._argv is None:
                self._argv = os.path.splitext(os.path.basename(self._path))[0]

            self._execute(self._path, self._argv, self._environ)
        except Exception as error:
            print('{} execution error: {}'.format(self.__class__.__name__, error), file=sys.stderr)

    if sys.platform == 'win32':

        @staticmethod
        def _execute(path, argv=None, environ=None):
            """ Executes an external search command.

            :param path: Path to the external search command.
            :type path: unicode

            :param argv: Argument list.
            :type argv: list or tuple
            The arguments to the child process should start with the name of the command being run, but this is not
            enforced. A value of :const:`None` specifies that the base name of path name :param:`path` should be used.

            :param environ: A mapping which is used to define the environment variables for the new process.
            :type environ: dict or None.
            This mapping is used instead of the current process’s environment. A value of :const:`None` specifies that
            the :data:`os.environ` mapping should be used.

            :return: None

            """
            from signal import signal, SIGBREAK, SIGINT, SIGTERM
            from subprocess import Popen
            import atexit

            def terminated(signal_number, frame):
                sys.exit('External search command terminated on receipt of signal={}.'.format(signal_number))

            process = Popen(argv, executable=path, env=environ, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr)
            atexit.register(lambda: process.kill if process.returncode else None)
            signal(SIGBREAK, terminated)
            signal(SIGINT, terminated)
            signal(SIGTERM, terminated)
            process.wait()

            sys.exit(process.returncode)

        @staticmethod
        def _search_path(executable, paths):
            """ Locates an executable program file.

            :param executable: The name of the executable program to locate.
            :type executable: unicode

            :param paths: A list of one or more directory paths where executable programs are located.
            :type paths: unicode

            :return:
            :rtype: Path to the executable program located or :const:`None`.

            """
            directory, filename = os.path.split(executable)
            extension = os.path.splitext(filename)[1].upper()
            executable_extensions = ExternalSearchCommand._executable_extensions

            if directory:
                if len(extension) and extension in executable_extensions:
                    return None
                for extension in executable_extensions:
                    path = executable + extension
                    if os.path.isfile(path):
                        return path
                return None

            if not paths:
                return None

            directories = [directory for directory in paths.split(';') if len(directory)]

            if len(directories) == 0:
                return None

            if len(extension) and extension in executable_extensions:
                for directory in directories:
                    path = os.path.join(directory, executable)
                    if os.path.isfile(path):
                        return path
                return None

            for directory in directories:
                path_without_extension = os.path.join(directory, executable)
                for extension in executable_extensions:
                    path = path_without_extension + extension
                    if os.path.isfile(path):
                        return path

            return None

        _executable_extensions = ('.COM', '.EXE')
    else:
        _execute = os.execvpe

    # endregion


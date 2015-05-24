#!/usr/bin/env python
# coding=utf-8
#
# Copyright © Splunk, Inc. All rights reserved.

from __future__ import absolute_import, division, print_function, unicode_literals
import os

if os.name == 'nt':

    def patch_os():
        import ctypes

        kernel32 = ctypes.windll.kernel32
        format_error = ctypes.FormatError

        create_hard_link = kernel32.CreateHardLinkW
        create_symbolic_link = kernel32.CreateSymbolicLinkW
        delete_file = kernel32.DeleteFileW
        get_file_attributes = kernel32.GetFileAttributesW
        remove_directory = kernel32.RemoveDirectoryW

        def islink(path):
            attributes = get_file_attributes(path)
            if attributes == -1:
                raise OSError(format_error())
            return (attributes & 0x400) != 0  # 0x400 == FILE_ATTRIBUTE_REPARSE_POINT

        os.path.islink = islink

        def link(source, link_name):
            if create_hard_link(link_name, source, None) == 0:
                raise OSError(format_error())

        os.link = link

        def remove(path):
            attributes = get_file_attributes(path)
            if attributes == -1:
                success = False
            elif (attributes & 0x400) == 0:  # file or directory, not symbolic link
                success = delete_file(path) != 0
            elif (attributes & 0x010) == 0:  # symbolic link to file
                success = delete_file(path) != 0
            else:                            # symbolic link to directory
                success = remove_directory(path) != 0
            if success:
                return
            raise OSError(format_error())

        os.remove = remove

        def symlink(source, link_name):
            if create_symbolic_link(link_name, source, 1 if os.path.isdir(source) else 0) == 0:
                raise OSError(format_error())

        os.symlink = symlink

    patch_os()
    del locals()['patch_os']  # since this function has done its job

import requests
import shutil
import sys

from subprocess import CalledProcessError, check_call, STDOUT
from distutils.core import setup, Command
from collections import OrderedDict
from glob import glob
from itertools import chain

project_dir = os.path.dirname(os.path.abspath(__file__))

# region Helper functions

def _splunk(*args):
    check_call(chain(('splunk', ), args), stderr=STDOUT, stdout=sys.stdout)
    return


def _splunk_restart(uri, auth):
    _splunk('restart', "-uri", uri, "-auth", auth)

# endregion

# region Command definitions


class AnalyzeCommand(Command):
    """ 
    setup.py command to run code coverage of the test suite. 

    """
    description = 'Create an HTML coverage report from running the full test suite.'

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            from coverage import coverage
        except ImportError:
            print('Could not import the coverage package. Please install it and try again.')
            exit(1)
            return
        c = coverage(source=['splunklib'])
        c.start()
        # TODO: instantiate and call TestCommand
        # run_test_suite()
        c.stop()
        c.html_report(directory='coverage_report')


class BuildCommand(Command):
    """
    setup.py command to create the application package file.

    """
    description = 'Package the app for distribution.'

    user_options = [
        (b'build-number=', None,
         'Build number (default: private)'),
        (b'debug-client=', None,
         'Copies the file at the specified location to package/bin/_pydebug.egg and bundles it and _pydebug_conf.py '
         'with the app'),
        (b'force', b'f',
         'Forcibly build everything')]

    def __init__(self, dist):

        Command.__init__(self, dist)

        self.package_name = '-'.join((self.distribution.metadata.name, self.distribution.metadata.version))
        self.build_number = 'private'
        self.debug_client = None
        self.build_base = None
        self.build_dir = None
        self.build_lib = None
        self.force = None

        return

    def initialize_options(self):
        return

    def finalize_options(self):

        self.package_name = self.package_name + '-' + unicode(self.build_number)
        self.build_base = os.path.join(project_dir, 'build')
        self.build_dir = os.path.join(self.build_base, self.distribution.metadata.name)
        self.build_lib = self.build_dir

        return

    def run(self):

        if self.debug_client is not None:
            shutil.copy(self.debug_client, os.path.join(self.distribution.package_dir[b'bin'], '_pydebug.egg'))

        if self.force and os.path.isdir(self.build_dir):
            shutil.rmtree(self.build_dir)

        self.run_command('build_py')
        self._copy_package_data()
        self._copy_data_files()
        self._make_archive()

        return

    def _copy_data_files(self):
        for directory, path_list in self.distribution.data_files:
            target = os.path.join(self.build_dir, directory)
            if not os.path.isdir(target):
                os.makedirs(target)
            for path in path_list:
                for source in glob(path):
                    if os.path.isfile(source):
                        shutil.copy(source, target)
                    pass
                pass
            pass
        return

    def _copy_package_data(self):
        for directory, path_list in self.distribution.package_data.iteritems():
            target = os.path.join(self.build_dir, directory)
            if not os.path.isdir(target):
                os.makedirs(target)
            for path in path_list:
                for source in glob(path):
                    if os.path.isfile(source):
                        shutil.copy(source, target)
                    pass
                pass
            pass
        return

    def _make_archive(self):
        import tarfile

        build_dir = os.path.basename(self.build_dir)
        archive_name = self.package_name + '.tar'
        current_dir = os.getcwdu()
        os.chdir(self.build_base)

        try:
            # We must convert the archive_name and base_dir from unicode to utf-8 due to a bug in the version of tarfile
            # that ships with Python 2.7.2, the version of Python used by the app team's build system as of this date:
            # 12 Sep 2014.
            tar = tarfile.open(str(archive_name), b'w|gz')
            try:
                tar.add(str(build_dir))
            finally:
                tar.close()
            gzipped_archive_name = archive_name + '.gz'
            if os.path.exists(gzipped_archive_name):
                os.remove(gzipped_archive_name)
            os.rename(archive_name, gzipped_archive_name)
        finally:
            os.chdir(current_dir)

        return


class LinkCommand(Command):
    """
    setup.py command to create a symbolic link to the app package at $SPLUNK_HOME/etc/apps.

    """
    description = 'Create a symbolic link to the app package at $SPLUNK_HOME/etc/apps.'

    user_options = [
        (b'debug-client=', None, 'Copies the specified debug client egg to package/_pydebug.egg'),
        (b'splunk-home=', None, 'Overrides the value of SPLUNK_HOME.')]

    def __init__(self, dist):
        Command.__init__(self, dist)

        self.debug_client = None
        self.splunk_home = os.environ['SPLUNK_HOME']
        self.app_name = self.distribution.metadata.name
        self.app_source = os.path.join(project_dir, 'package')

        return

    def initialize_options(self):
        pass

    def finalize_options(self):
        return

    def run(self):
        target = os.path.join(self.splunk_home, 'etc', 'apps', self.app_name)

        if os.path.islink(target):
            os.remove(target)
        elif os.path.exists(target):
            message = 'Cannot create a link at "{}" because a file by that name already exists.'.format(target)
            raise SystemError(message)

        self._copy_debug_client()
        self._copy_lookups()
        self._link_packages()

        os.symlink(self.app_source, target)
        return

    def _copy_debug_client(self):
        if not self.debug_client:
            return
        shutil.copy(self.debug_client, os.path.join(self.app_source, 'bin', '_pydebug.egg'))

    def _copy_lookups(self):
        lookups_dir = os.path.join(self.app_source, 'lookups')

        if not os.path.isdir(lookups_dir):
            os.mkdir(lookups_dir)

        random_data = os.path.join(lookups_dir, 'random_data.csv.gz')

        if not os.path.isfile(random_data):
            download = 'http://splk-newtest-data.s3.amazonaws.com/chunked_external_commands/lookups/random_data.csv.gz'
            response = requests.get(download)
            with open(random_data, 'wb') as output:
                output.write(response.content)
            pass

        return

    def _link_packages(self):
        path = os.path.join(self.app_source, 'bin', 'packages')
        if not os.path.isdir(path):
            os.mkdir(path)
        path = os.path.join(path, 'splunklib')
        if not os.path.islink(path):
            os.symlink(os.path.realpath(os.path.join(project_dir, '..', '..', 'splunklib')), path)
        return


class TestCommand(Command):
    """ 
    setup.py command to run the whole test suite. 

    """
    description = 'Run full test suite.'

    user_options = [
        (b'commands=', None, 'Comma-separated list of commands under test or *, if all commands are under test'),
        (b'build-number=', None, 'Build number for the test harness'),
        (b'auth=', None, 'Splunk login credentials'),
        (b'uri=', None, 'Splunk server URI'),
        (b'env=', None, 'Test running environment'),
        (b'pattern=', None, 'Pattern to match test files'),
        (b'skip-setup-teardown', None, 'Skips test setup/teardown on the Splunk server')]

    def __init__(self, dist):
        Command.__init__(self, dist)

        self.test_harness_name = self.distribution.metadata.name + '-test-harness'
        self.uri = 'https://localhost:8089'
        self.auth = 'admin:changeme'
        self.env = 'test'
        self.pattern = 'test_*.py'
        self.skip_setup_teardown = False

        return

    def initialize_options(self):
        pass  # option values must be initialized before this method is called (so why is this method provided?)

    def finalize_options(self):
        pass

    def run(self):
        import unittest

        if not self.skip_setup_teardown:
            try:
                _splunk(
                    'search', '| setup environment="{0}"'.format(self.env), '-app', self.test_harness_name,
                    '-uri', self.uri, '-auth', self.auth)
                _splunk_restart(self.uri, self.auth)
            except CalledProcessError as e:
                sys.exit(e.returncode)

        current_directory = os.path.abspath(os.getcwd())
        os.chdir(os.path.join(project_dir, 'tests'))
        print('')

        try:
            suite = unittest.defaultTestLoader.discover('.', pattern=self.pattern)
            unittest.TextTestRunner(verbosity=2).run(suite)  # 1 = show dots, >1 = show all
        finally:
            os.chdir(current_directory)

        if not self.skip_setup_teardown:
            try:
                _splunk('search', '| teardown', '-app', self.test_harness_name, '-uri', self.uri, '-auth', self.auth)
            except CalledProcessError as e:
                sys.exit(e.returncode)

        return

# endregion

setup(
    description='Application for testing the Chunked Search Commands feature',
    name=os.path.basename(project_dir),
    version='1.0.0',
    author='Splunk, Inc.',
    author_email='devinfo@splunk.com',
    url='http://github.com/splunk/splunk-sdk-python',
    license='http://www.apache.org/licenses/LICENSE-2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Other Environment',
        'Intended Audience :: Information Technology',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: System :: Logging',
        'Topic :: System :: Monitoring'],
    packages=[
        b'bin.packages.splunklib', b'bin.packages.splunklib.searchcommands'
    ],
    package_dir={
        b'bin': 'package/bin',
        b'bin.packages': 'package/bin/packages',
        b'bin.packages.splunklib': '../../splunklib',
        b'bin.packages.splunklib.searchcommands': '../../splunklib/searchcommands',
    },
    package_data={
        b'bin': [
            'package/bin/_pydebug.egg',
            'package/bin/_pydebug_conf.py',
            'package/bin/app.py',
            'package/bin/countmatches.py',
            'package/bin/echo.py',
            'package/bin/generatehello.py',
            'package/bin/pypygeneratehello.py',
            'package/bin/simulate.py',
            'package/bin/sum.py'
        ]
    },
    data_files=[
        (b'default', ['package/default/*.conf']),
        (b'lookups', ['package/lookups/*.csv.gz']),
        (b'metadata', ['package/metadata/default.meta'])
    ],
    cmdclass=OrderedDict([
        ('analyze', AnalyzeCommand),
        ('build', BuildCommand),
        ('link', LinkCommand),
        ('test', TestCommand)]))

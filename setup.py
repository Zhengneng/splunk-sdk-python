#!/usr/bin/env python
# coding=utf-8
#
# Copyright © Splunk, Inc. All rights reserved.

from __future__ import absolute_import, division, print_function, unicode_literals
import os
import requests
import shutil
import sys

from subprocess import CalledProcessError, check_call, STDOUT
from distutils.core import setup, Command
from itertools import chain

project_dir = os.path.dirname(os.path.abspath(__file__))


# region Helper functions

def _make_archive(package_name, build_dir, base_dir):
    import tarfile

    basename, extension = os.path.splitext(package_name)
    archive_name = basename + '.tar'
    current_dir = os.getcwd()
    os.chdir(build_dir)

    try:
        # We must convert the archive_name and base_dir from unicode to utf-8 due to a bug in the version of tarfile
        # that ships with Python 2.7.2, the version of Python used by the app team's build system as of this date:
        # 12 Sep 2014.
        tar = tarfile.open(str(archive_name), b'w|gz')
        try:
            tar.add(str(base_dir))
        finally:
            tar.close()
        os.rename(archive_name, package_name)
    finally:
        os.chdir(current_dir)

    return


def _package_app(name, source, metadata, build_dir, package_name, debug_client=False):
    import stat

    temp_dir = os.path.join(build_dir, name)

    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)

    ignore_patterns = ['.*', '*.pyc', '*.swp', 'local', 'local.meta']

    if not debug_client:
        ignore_patterns.extend(('_debug_conf.py', 'pydebug.egg'))

    shutil.copytree(source, temp_dir, ignore=shutil.ignore_patterns(*ignore_patterns))
    default_dir = os.path.join(temp_dir, 'default')

    for filename in os.listdir(default_dir):
        if filename.endswith('.conf'):
            path = os.path.join(default_dir, filename)
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
            with open(path, 'r+b') as f:
                format_spec = ''.join(f.readlines())
                text = format_spec.format(**vars(metadata))
                f.seek(0)
                f.truncate()
                f.writelines((text.encode('utf-8'),))
            pass

    package_path = os.path.join(build_dir, package_name)

    if os.path.exists(package_path):
        os.remove(package_path)

    _make_archive(package_name, build_dir, base_dir=name)
    shutil.rmtree(temp_dir)

    return os.path.join(build_dir, package_name)


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


class PackageCommand(Command):
    """ 
    setup.py command to create the application package file. 

    """
    description = 'Package the app for distribution.'

    user_options = [
        (b'build-number=', None, 'Build number (default: private)'),
        (b'debug-client', None, 'Packages the PyCharm debug client egg with the app')]

    def __init__(self, dist):
        Command.__init__(self, dist)

        self.app_version = self.distribution.metadata.version

        self.app_name = self.distribution.metadata.name
        self.app_source = os.path.join(project_dir, 'package')
        self.app_package_name = '-'.join((self.app_name, self.app_version))

        self.build_number = 'private'
        self.debug_client = False
        self.target = 'app'

        return

    def initialize_options(self):
        pass

    def finalize_options(self):
        self.target = {target.strip(' ') for target in self.target.split(',')}
        diff = self.target.difference({'app', 'test-harness'})
        if diff:
            print('Unrecognized target: {0}'.format(','.join(diff)))
            exit(1)
        return

    def run(self):

        package_name = '-'.join((self.app_package_name, unicode(self.build_number) + '.tgz'))
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

        _package_app(
            self.app_name, source=self.app_source, metadata=self.distribution.metadata, build_dir=project_dir,
            package_name=package_name, debug_client=self.debug_client)

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
        (b'skip-setup-teardown', None, 'Skips SA-ldapsearch test setup/teardown on the Splunk server')]

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
    cmdclass={'analyze': AnalyzeCommand, 'package': PackageCommand, 'test': TestCommand},
    description='Application for testing the Chunked External Search Commands feature',
    name='chunked_external_commands',
    version='1.0.0',
    author='Splunk, Inc.',
    author_email='dnoble@splunk.com',
    url='',
    license='',
    packages=[],
    package_dir={'': 'bin/packages'},

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Other Environment',
        'Intended Audience :: Information Technology',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: System :: Logging',
        'Topic :: System :: Monitoring'])

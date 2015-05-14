# coding=utf-8
#
# Copyright © Splunk, Inc. All Rights Reserved

from splunklib.searchcommands.external_search_command import execute
import os

os.environ.pop('DYLD_LIBRARY_PATH', None)
execute('pypy', (generatehello.py,))

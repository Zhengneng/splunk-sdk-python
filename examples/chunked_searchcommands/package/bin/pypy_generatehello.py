from __future__ import absolute_import, division, print_function, unicode_literals
import os
import sys

os.environ.pop('DYLD_LIBRARY_PATH', None)
print('PYPY: os.environ = ' + unicode(os.environ), file=sys.stderr)

app_root = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))
print('PYPY: app_root = ' + app_root, file=sys.stderr)

argv = ['pypy', os.path.join(app_root, 'bin', 'generatehello.py')] + sys.argv[1:]
print('PYPY: argv = ' + unicode(argv), file=sys.stderr)

try:
    os.execvp('pypy', argv)
except Exception as error:
    print('PYPY: ' + unicode(error))

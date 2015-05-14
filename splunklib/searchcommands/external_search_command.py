import __main__
import os
import sys

def execute(path, argv=None, environ=None, app_root=None):
    ExternalSearchCommand().execute(path, argv, environ, app_root)

class ExternalSearchCommand(object)
    
    def __init__(self):
        self._app_root = os.path.dirname(os.path.abspath(os.path.dirname(__main__.__file__)))
        self._path = None
        self._argv = None
        self._environ = os.environ

    # region Properties

    @property
    def app_root(self):
        return self._app_root

    @app_root.setter
    def app_root(self, value):
        if not (isinstance(value, basestring) and os.path.isdir(value)):
            raise ValueError('Expected an existing directory path for app_root, not {}', repr(value))
        self._app_root = value

    @property
    def argv(self):
        return self._argv

    @argv.setter
    def argv(self, value):
        if not (value is None or isinstance(value, (list, tuple))):
            raise ValueError('Expected a list, tuple or value of None for environ, not {}', repr(value))
        return self._argv = value

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
        if not isinstance(basestring):
            raise ValueError('Expected a string value for path, not {}', repr(value))
        self._path = value

    # endregion

    # region Methods

    def execute(self, path=None, argv=None, environ=None, app_root=None)
        try:
            if path is not None:
                self.path = path
            if argv is not None:
                self.argv = argv
            if environ is not None:
                self.environ = environ
            if app_root is not None:
                self.app_root = app_root
            os.execvp(self._path, self._argv, self._environ)
        except Exception as error:
            print('{} execution error: {}'.format(self.__class__.__name__, error), file=sys.stderr)

    # endregion

import os
from inspect import getfile, currentframe
ROOT = os.path.dirname(os.path.abspath(getfile(currentframe())))
BASE = os.path.join(ROOT, '..')
BUNDLE = os.path.join(ROOT, 'bundles')
LOG = os.environ.get('LOGDIR', os.path.join(BASE, 'logs'))
CONFIG = os.environ.get('CONFIGFILE', os.path.join(BASE, 'config.ini'))
DEBUG = True if os.environ.get('DEBUG') == 'true' else False
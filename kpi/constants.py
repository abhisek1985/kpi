import os
from inspect import getfile, currentframe
ROOT = os.path.dirname(os.path.abspath(getfile(currentframe())))
BASE = os.path.join(ROOT, '..')
CONFIG = os.environ.get('CONFIGFILE', os.path.join(BASE, 'config.ini'))
BUNDLE = os.path.join(ROOT, 'bundle')
TABLES = [
    'property_analytics',
    'user_info',
    'user_info_detailed',
    'property_search',
    'property_visits',
    'subscribers',
    'page_view_and_scroll',
    'page_view_and_scroll_details'
]


class Constants:
    DEBUG = False

    def __init__(self, configClass):
        # Constants.config_data = configClass.conf
        debug = configClass.conf['DEFAULT'].get('Debug_Mode', 'false')
        if debug == 'true':
            Constants.DEBUG = True

    # BUNDLE = os.path.join(ROOT, 'bundles')
    # LOG = os.environ.get('LOGDIR', os.path.join(BASE, 'logs'))
    # DEBUG = config_data['DEFAULT'].get('Debug_Mode', 'false')

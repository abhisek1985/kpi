"""

"""
from six.moves import configparser


class ConfigParser:
    """

    """
    conf = configparser.ConfigParser()

    def __init__(self, config_file):
        """
        :param config_file:
        """
        # PWD = os.path.dirname(os.path.abspath(getfile(currentframe())))
        # if os.path.exists(PWD + '/' + config_file):
        #     ConfigParser.conf.read(PWD + '/' + config_file)
        # else:
        #     raise OSError('Config file is not present')
        ConfigParser.conf.read(config_file)
        if len(ConfigParser.conf.sections()) != 3:
            raise ValueError('Unnecessary values in config file')

        if 'DB' not in ConfigParser.conf.sections():
            raise ValueError('Database configuration is not found')
        else:
            self.dbType = ConfigParser.conf['DB'].get('Db', None)
            self.port = int(ConfigParser.conf['DB'].get('Port', -1))
            self.dbName = ConfigParser.conf['DB'].get('Dbname', None)
            self.password = ConfigParser.conf['DB'].get('Pass', None)
            self.user = ConfigParser.conf['DB'].get('User', None)
            self.host = ConfigParser.conf['DB'].get('Host', None)
            self.enc = ConfigParser.conf['DB'].get('Encryption', None)
            if self.enc == 'true':
                self.enc = True
            else:
                self.enc = False
        if ConfigParser.conf['DEFAULT']['Authentication'] == 'true':
            if 'AUTH' not in ConfigParser.conf.sections():
                raise ValueError('Authentication settings to API is not found')
            else:
                self.no_allwd_conn = ConfigParser.conf['DB'].get('nac', 100)
                self.certs = ConfigParser.conf['DB'].get('certs', '.')
        elif ConfigParser.conf['DEFAULT']['Logging'] == 'true':
            if 'LOG' not in ConfigParser.conf.sections():
                raise ValueError('Authentication settings to API is not found')
            else:
                self.logfile = ConfigParser.conf['LOG'].get('log', 'api.log')

    @property
    def dbinfo(self):
        """

        :return:
        """
        return {
            'DB_type': self.dbType,
            'port': self.port,
            'database': self.dbName,
            'pass': self.password,
            'user': self.user,
            'host': self.host,
            'encryption': self.enc
        }

    # @property
    # def auth_info(self):
    #     """
    #
    #     :return:
    #     """
    #     return {
    #         'num_of_conn': self.num_allowed_conn,
    #         'certs': self.certificates
    #     }
    #
    # @property
    # def log_info(self):
    #     """
    #
    #     :return:
    #     """
    #     return {
    #         'logfile': self.logfile,
    #         'logType': self.logType,
    #         'log_format': self.log_format
    #     }

from .mysql import MySQL
from .config import ConfigParser
from .constants import CONFIG, Constants


def parse_config(config_file):
    return ConfigParser(config_file)


def get_parser_info(database_info):
    host = database_info['host']
    port = database_info['port']
    dbName = database_info['database']
    user = database_info['user']
    password = database_info['pass']
    enc = database_info['encryption']
    return MySQL(
        host=host,
        prt=port,
        epass=password,
        dbname=dbName,
        user=user,
        encryption=enc
    )


class DBBuffer:
    def __init__(self):
        parser = parse_config(CONFIG)
        self.db_info = get_parser_info(database_info=parser.dbinfo)
        Constants(configClass=ConfigParser)

    def custom_read(self, rows, cols, return_type, table):
        empty_rows = not rows or rows == ()
        empty_cols = not cols or cols == ()
        # return_type == 'DataFrame'
        if all([empty_rows, empty_cols, return_type == 'DataFrame']):
            return self[table]
        return self.db_info.read(
            rows=rows,
            cols=cols,
            return_type=return_type,
            table=table)

    def __getitem__(self, table):
        return self.db_info.read(
            table=table,
            return_type='DataFrame'
        )

"""
This module is used to provide
a bridge connection between the
data base and the internal APIs.
The purpose of this module is to
provide the set base classes that
can add some enhancements to the
database related APIs.
"""
from abc import (ABC, abstractmethod)
import pymysql
import sys
import base64

from pymysql.cursors import SSCursor


class Database(ABC):
    """
    This class acts as an abstract base class
    to implement the basic operation for a database
    after authentication is performed. This gives out
    basic CRUD functionality in a form of abstract methods.
    """

    def __init__(self, dbtype, user, host, prt, dbname, epass):
        """
        Initializer class
        :param dbtype:
        :param host:
        :param port:
        :param dbname:
        :param epass:
        """
        self.user = user
        self.dbtype = dbtype
        self.host = host
        self.port = prt
        self.dbname = dbname
        self.password = epass

        try:
            self.connector = pymysql.connect(host=host,
                                             user=user,
                                             passwd=base64.b64decode(epass).decode("utf-8"),
                                             db=dbname,
                                             port=prt,
                                             cursorclass=SSCursor)
        except pymysql.MySQLError as e:
            print('Got error {!r}, errno is {}'.format(e, e.args[0]),
                  file=sys.stderr)
        else:
            self.cursor = self.connector.cursor()

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def read(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def delete(self):
        pass

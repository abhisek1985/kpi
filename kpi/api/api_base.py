"""
This is the base api class. This prepares
the development ground for creating API.
All the API plugins need to import the
classes from the this. This means all
the API plugins needs to access this
module and it should not be changed
unless it is absolutely known.
"""

from ..dispatch import DBBuffer
from ..core import preprocess
from ..IO import buffer_classes
import pandas as pd


class API(DBBuffer):
    """
    The main API class that would
    be used to access the dataset
    from the database. All the access
    parameters of server end and API
    access control tuned from here.
    This inherits the DBBuffer class
    which gets the data from the database.
    The __getitem__ overridden method can
    be used with the table name to get data.
    Read the documentation of DBBuffer class.
    """

    def __init__(self, base):
        super(self.__class__, self).__init__()
        if base not in ['django', 'flask']:
            raise ValueError('Endpoint is not supported yet!!')


preprocess = preprocess
# stdout_wrap = buffer_classes.Overridden(override=sys.stdout)
json_wrap = buffer_classes.JSONWrap
dict_wrap = buffer_classes.DictWrap
# csv_wrap = buffer_classes.CSVWrap(klass='CSV')
# excel_wrap = buffer_classes.EXCELWrap(klass='EXCEL')


class GetData:
    """
    Decorator pattern for getting data
    from a particular table. Loads the
    if it is not already loaded to the
    memory. This speeds up the process
    of data loading but hurts the memory
    consumption. But who cares for memory.
    """
    data = dict()

    def __init__(self, table):
        self.table = table

    def __call__(self, func):
        def wrapper(*args):
            if not hasattr(GetData.data, self.table):
                GetData.data[self.table] = API(base='django')[self.table]
            return func(*args)
        return wrapper

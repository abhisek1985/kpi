from .db_driver import Database
import pandas as pd


class MySQL(Database):

    def __init__(self, user, host, prt, epass, dbname):
        super(self.__class__, self).__init__(
            dbtype="MySQL",
            user=user,
            host=host,
            prt=prt,
            dbname=dbname,
            epass=epass
        )

    def col_info(self, table):
        """
        Get names of all the column values
        of an SQL table.

        :param table: Name of the table
        :return list: Returns the list of column values
        """
        self.cursor.execute("DESC {};".format(table))
        table_desc = self.cursor.fetchall()
        return map(lambda x: x[0], table_desc)

    def shape(self, table):
        """
        Returns the shape of the SQL table.

        :param table: Name of the SQL table
        :return tuple: returns the tuple containing row and column
        """
        select = "SELECT count(*) FROM INFORMATION_SCHEMA.COLUMNS"
        condition = " WHERE table_schema = "
        clause = "'{}' AND table_name = '{}';".format(self.dbname, table)
        count = select + condition + clause
        self.cursor.execute("SELECT COUNT(*) FROM {}.{};".format(self.dbname,table))
        n_rows = self.cursor.fetchall()
        self.cursor.execute(count)
        n_cols = self.cursor.fetchall()
        return n_rows[0][0], n_cols[0][0]

    def create(self):
        raise NotImplementedError('Not Implemented as handled by Backend')

    def _read_all(self, table, how='optimised'):
        """
        Reads all the rows from ```table```. If we
        keep ```how``` is optimised, then it returns
        a memory efficient iterator or else it returns
        the copy of the data.

        :param table: Table to read data from
        :param how: Reading type
        :return: iterator or data copy
        """
        self.cursor.execute("SELECT * FROM {};".format(table))
        if how == 'optimised':
            return self.cursor.fetchall_unbuffered()
        else:
            return self.cursor.fetchall()

    def read(self, rows=None, cols=None, return_type='DataFrame', table=None):
        """
        Upper level API to call for read. It also performs
        indexing in row and column. The return type can also
        be changed. We can mention table name to pull the data
        from.
        :param rows: tuple (upper, lower) or () or None ->
            Lower range and Upper range
        :param cols: tuple (upper, lower) or () or None ->
            Lower range and Upper range
        :param return_type: dict() or data_frame() or iterator()
        :return: indexed or non indexed data from the table
        """
        if table is None:
            raise ValueError("Table name must be provided")
        if return_type == 'iterator':
            return self._read_all(table=table)

        if rows is None or rows == ():
            all_rows_return = True
        else:
            all_rows_return = False
        if cols is None or cols == ():
            all_cols_return = True
        else:
            all_cols_return = False

        if not all_rows_return:
            if rows[0] < 0 and rows[1] < 0:
                raise ValueError("Negative index is not allowed")
            if rows[0] >= rows[1]:
                raise ValueError("Invalid rows input")

        if not all_cols_return:
            if cols[0] < 0 and cols[1] < 0:
                raise ValueError("Negative index is not allowed")
            if cols[0] >= cols[1]:
                raise ValueError("Invalid cols input")

        # Getting the data and creating the data frame
        col_list = list(self.col_info(table=table))
        buffer = self._read_all(table=table, how='greedy')
        data = pd.DataFrame(buffer, columns=col_list)

        # Indexing on rows & cols if required
        r, c = self.shape(table=table)
        if not all([all_cols_return, all_rows_return]):
            if all_rows_return:
                icc = cols[0] not in range(c) or cols[1] not in range(c)
                if icc:
                    raise ValueError("Invalid cols input")
                data = data.iloc[cols[0]:cols[1], ]

            elif all_cols_return:
                irc = rows[0] not in range(r) or rows[1] not in range(r)
                if irc:
                    raise ValueError("Invalid rows input")
                data = data.iloc[rows[0]:rows[1], ]

            else:
                icc = cols[0] not in range(c) or cols[1] not in range(c)
                irc = rows[0] not in range(r) or rows[1] not in range(r)
                if irc and icc:
                    raise ValueError("Invalid rows and cols input")
                data = data.iloc[rows[0]:rows[1], cols[0]:cols[1]]

        # Return the final dataset
        if return_type == 'DataFrame':
            return data
        else:
            return data.to_dict('records')

    def update(self):
        raise NotImplementedError('Not Implemented as handled by Backend')

    def delete(self):
        raise NotImplementedError('Not Implemented as handled by Backend')

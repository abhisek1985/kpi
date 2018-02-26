import pandas as pd
from .ops import OPERATIONS
from math import log10, floor
import numpy as np
import pandas as pd
import pandas as pd


def _round_off_10k(num):
    exponent = log10(num)
    if exponent != float(floor(exponent)):
        return 10 ** floor(exponent) + 10 ** (floor(exponent) - 1)
    return num


def _create_interval(data, col_name, used_cols, step):
    if len(used_cols) != 1:
        raise ValueError("Only one column is supported for this operation")
    data_strip = data[used_cols[0]]
    max_val = _round_off_10k(data_strip.max())
    min_val = _round_off_10k(data_strip.min())
    interval = np.arange(min_val, max_val + min_val, step)
    bins = zip(interval, interval[1::])
    bins_text = list(map(lambda x: "{} to {}".format(x[0], x[1]), bins))
    new_col_data = pd.cut(
        data.listing_price,
        bins=interval,
        labels=bins_text,
        include_lowest=True)
    data[col_name] = new_col_data


def create_col(data, col_name=None, used_cols=None, operation=None, **kwargs):
    if data.shape[0]:
        if used_cols is None:
            raise ValueError('At least one column is required')
        elif col_name is None:
            raise ValueError('No value is given for new column name')

        if operation is None:
            raise ValueError('Operation has to be given')
        elif operation not in OPERATIONS:
            raise NotImplementedError('Given operation is not valid')
        else:
            if operation == 'CREATE_RANGE':
                interval = kwargs.get('interval', None)
                if interval is None:
                    err_msg = 'Interval {} can not be none'.format(operation)
                    raise ValueError(err_msg)
                else:
                    _create_interval(data, col_name, used_cols, interval)
                    return True
    return False


def get_percentage(data, col1, col2, new_col):
    data[new_col] = (data[col1] - data[col2]) / data[col1] * 100
    return True


def generic_operations(data, col1, col2, new_col, operation):
    if isinstance(col2, int):
        col2_data = col2
    else:
        col2_data = data[col2]

    if operation == '/':
        data[new_col] = data[col1] / col2_data
    elif operation == '*':
        data[new_col] = data[col1] * col2_data
    elif operation == '+':
        data[new_col] = data[col1] + col2_data
    elif operation == '-':
        data[new_col] = data[col1] - col2_data
    else:
        err_msg = 'Operation {} is not supported yet!!'.format(operation)
        raise NotImplementedError(err_msg)


def _encode_month(data):
    month_table = [
        None, 'Jan', 'Feb', 'Mar',
        'Apr', 'May', 'Jun',
        'Jul', 'Aug', 'Sept',
        'Oct', 'Nov', 'Dec'
    ]
    data['month_enc'] = data.apply(
        lambda row: month_table[row['month']], axis=1)
    return True


def _encode_q(data):
    q_table = [
        None, 'Quarter 1', 'Quarter 2',
        'Quarter 3', 'Quarter 4'
    ]
    data['quarter_enc'] = data.apply(
        lambda row: q_table[row['quarter']], axis=1)
    return True


def extract_timeseries(data, into=None, **kwargs):
    ts_col = kwargs.get('ts_col')
    if not ts_col:
        return False

    if into != 'minutes':
        data[ts_col] = pd.to_datetime(data[ts_col])

    if into == 'month':
        data['month'] = data[ts_col].dt.month
    elif into == 'week':
        data['week_enc'] = data[ts_col].dt.week
    elif into == 'quarter':
        data['quarter'] = data[ts_col].dt.quarter
    elif into == 'minutes':
        data['minutes'] = data[ts_col] / np.timedelta64(1, 'm')
    elif into:
        return False

    if into and into != 'minutes':
        encode = kwargs.get('encode', False)
        if encode:
            if into == 'month':
                return _encode_month(data)
            elif into == 'quarter':
                # pass
                return _encode_q(data)
    return True


def make_frame(data, **kwargs):
    index = kwargs.get('index')
    if not index:
        return pd.DataFrame(data)
    else:
        return pd.DataFrame(data, index=index)

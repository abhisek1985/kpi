from .api_base import get_data
from ..constants import Constants
from six import reraise as raise_
from .api_base import preprocess
from .api_base import json_wrap, dict_wrap


API_TABLE_1 = 'user_info'
API_TABLE_2 = 'user_info_detailed'

data_user_info = get_data(API_TABLE_1)
data_user_info_det = get_data(API_TABLE_2)


def historical_property_views(
        filter_col,
        target_group,
        time_aggregation='month'):
    """
    This show count of houses viewed over time from
    the target customers mentioned by ```target_group``` and
    aggregate them over timeline mentioned by ```time_aggregation```

    :param filter_col: group of values (column and value) -> tuple
    :param target_group: The group of incoming customers
    :param time_aggregation: How to aggregate the metic over time
    """
    if time_aggregation not in ['month', 'week', 'quarter']:
        if Constants.DEBUG:
            err_msg = "Aggregate {} is not supported".format(time_aggregation)
            raise_(NotImplementedError, NotImplementedError(err_msg))
        return False, None

    # Apply the data filter
    data_temp = None
    data = data_user_info
    if filter_col and len(filter_col) == 2:
        try:
            fc = filter_col[0]
            fv = filter_col[1]
            data_temp = data[data[fc] == fv].copy(deep=True)
        except KeyError:
            if Constants.DEBUG:
                msg = 'filter column {} is not present'.format(filter_col[0])
                raise_(AttributeError, AttributeError(msg))
            else:
                return False, None
    if data_temp is None:
        data_temp = data.copy(deep=True)

    # Apply the target group
    data_temp = data_temp[data_temp.SearchedAs.isin(target_group)]
    if data_temp.shape[0] == 0:
        if Constants.DEBUG:
            print("The respective target_group returns not result")
        dwrap = dict_wrap(override=None)
        output = lambda: {}
        return True, dwrap.wrap(functor=output)

    # Create the time aggregation
    preprocess.extract_timeseries(
        data=data_temp,
        into=time_aggregation,
        ts_col='Session_in',
        encode=True)
    ret = data_temp.groupby(
        str(time_aggregation) + '_enc').UserID.count().reset_index()
    ret.columns = [str(time_aggregation), 'visits']
    output = lambda: ret
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)


def call_to_action(filter_col):
    data_temp = None
    data = data_user_info
    if filter_col and len(filter_col) == 2:
        try:
            fc = filter_col[0]
            fv = filter_col[1]
            data_temp = data[data[fc] == fv].copy(deep=True)
        except KeyError:
            if Constants.DEBUG:
                msg = 'filter column {} is not present'.format(filter_col[0])
                raise_(AttributeError, AttributeError(msg))
            else:
                return False, None
    if data_temp is None:
        data_temp = data.copy(deep=True)

    preprocess.extract_timeseries(
        data=data_temp,
        into='month',
        ts_col='Session_in',
        encode=True)
    ret = data_temp.melt(
        id_vars='month_enc',
        value_vars=['Emailed', 'Phone', 'IM', 'FaA'],
        var_name='Action', value_name='times')
    # return ret
    output = lambda: ret
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)


def historical_user_stats(filter_col, factor):
    data_temp = None
    data = data_user_info_det
    if filter_col and len(filter_col) == 2:
        try:
            fc = filter_col[0]
            fv = filter_col[1]
            data_temp = data[data[fc] == fv].copy(deep=True)
        except KeyError:
            if Constants.DEBUG:
                msg = 'filter column {} is not present'.format(filter_col[0])
                raise_(AttributeError, AttributeError(msg))
            else:
                return False, None
    if data_temp is None:
        data_temp = data.copy(deep=True)

    # print(data_temp)
    if factor == 'location':
        ret = data_temp.groupby(
            'location').PropertyVisitedForBuy.count().reset_index()
    elif factor == 'houses':
        ret = data_temp.groupby(
            'PropertyVisitedForBuy').UserID.sum().reset_index()
        ret.columns = ['houses', 'count']
        # ret.set_index('houses', inplace=True)
        # print(ret)
    else:
        if Constants.DEBUG:
            err_msg = "The factor {} is not supported".format(factor)
            raise_(NotImplementedError, NotImplementedError(err_msg))
        return False, None
    output = lambda: ret.set_index(factor)
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)


def average_search_time(filter_col, time_aggregation, metrics, **kwargs):
    if time_aggregation not in ['month', 'week', 'quarter']:
        if Constants.DEBUG:
            err_msg = "Aggregate {} is not supported".format(time_aggregation)
            raise_(NotImplementedError, NotImplementedError(err_msg))
        return False, None
    data_temp = None
    data = data_user_info_det
    if filter_col and len(filter_col) == 2:
        try:
            fc = filter_col[0]
            fv = filter_col[1]
            data_temp = data[data[fc] == fv].copy(deep=True)
        except KeyError:
            if Constants.DEBUG:
                msg = 'filter column {} is not present'.format(filter_col[0])
                raise_(AttributeError, AttributeError(msg))
            else:
                return False, None
    if data_temp is None:
        data_temp = data.copy(deep=True)
    preprocess.extract_timeseries(
        data=data_temp,
        into=time_aggregation,
        ts_col='Date',
        encode=True)
    another_agg = kwargs.get('other_agg', False)
    if another_agg and another_agg == 'location':
        ret = data_temp.groupby([
            str(time_aggregation) + '_enc',
            another_agg]).sum().reset_index()
    else:
        ret = data_temp.groupby([str(time_aggregation) + '_enc']).sum()
    output = lambda: ret[metrics]
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)

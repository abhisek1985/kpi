from .api_base import get_data
from .api_base import json_wrap, dict_wrap
from ..constants import Constants
from .api_base import preprocess
from six import reraise as raise_


API_TABLE_1 = 'property_visits'
data_kiss = get_data(API_TABLE_1)


def visiting_demography():
    heatmap = data_kiss.groupby(
        ['Visited_from', 'Location']).PropertyID.count().reset_index()
    ret = heatmap.pivot(
        'Visited_from',
        'Location',
        'PropertyID').fillna(0).astype(int).reset_index()
    output = lambda: ret.set_index('Visited_from')
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)


def top_n_visit_demography(n):
    ret = data_kiss.groupby(['Visited_from']).PropertyID.count().reset_index()
    ret.sort_values(by='PropertyID', ascending=False, inplace=True)
    ret = ret.head(n)
    if ret.shape[0] == 0:
        if Constants.DEBUG:
            print('The aggregation returned no data!')
        dwrap = dict_wrap(override=None)
        output = lambda: {}
        return True, dwrap.wrap(functor=output)
    ret = ret.reset_index(drop=True)
    ret.columns = ['Visited_from', 'count']
    output = lambda: ret.set_index('Visited_from')
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)


def average_session_length(aggregation=None):
    preprocess.extract_timeseries(
        data=data_kiss,
        ts_col='VisitingTimeStart')
    preprocess.extract_timeseries(
        data=data_kiss,
        ts_col='VisitingTimeEnd')
    preprocess.generic_operations(
        data=data_kiss,
        col1='VisitingTimeEnd',
        col2='VisitingTimeStart',
        new_col='time_diff',
        operation='-')
    preprocess.extract_timeseries(
        data=data_kiss,
        into='minutes',
        ts_col='time_diff')
    if aggregation and (aggregation not in data_kiss.columns.values):
        if Constants.DEBUG:
            err_msg = 'Column {} is not present'.format(aggregation)
            raise_(AttributeError, AttributeError(err_msg))
        return False, None
    elif aggregation:
        ret = data_kiss.groupby(aggregation).minutes.mean()
    else:
        ret = data_kiss.minutes.mean()
    if isinstance(ret, float):
        output = lambda: {'average_time': ret}
        if Constants.DEBUG:
            print(output())
            return True, None
        dwrap = dict_wrap(override=None)
        return True, dwrap.wrap(functor=output)
    else:
        ret = ret.reset_index()
        ret.columns = [aggregation, 'average_time']
        output = lambda: ret.set_index(aggregation)
        if Constants.DEBUG:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
        else:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)


def most_popular_pages(page, aggregation):
    if page not in ['EntryPage', 'ExitPage']:
        if Constants.DEBUG:
            err_msg = 'Mentioned page {} is not known!!'.format(page)
            raise_(NotImplementedError, NotImplementedError(err_msg))
        return False, None
    if aggregation not in ['average', 'total']:
        if Constants.DEBUG:
            err_msg = 'Passed aggregation {} not possible'.format(aggregation)
        return False, None
    preprocess.extract_timeseries(
        data=data_kiss,
        ts_col='VisitingTimeStart')
    preprocess.extract_timeseries(
        data=data_kiss,
        ts_col='VisitingTimeEnd')
    preprocess.generic_operations(
        data=data_kiss,
        col1='VisitingTimeEnd',
        col2='VisitingTimeStart',
        new_col='time_diff',
        operation='-')
    preprocess.extract_timeseries(
        data=data_kiss,
        into='minutes',
        ts_col='time_diff')
    if aggregation == 'total':
        ret = data_kiss.groupby(page).minutes.sum()
    else:
        ret = data_kiss.groupby(page).minutes.mean()
    ret = ret.reset_index()
    output = lambda: ret.sort_values(by='minutes', ascending=False)
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)

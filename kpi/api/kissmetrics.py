from .api_base import get_data
from .api_base import json_wrap, dict_wrap
from ..constants import Constants
from .api_base import preprocess
from six import reraise as raise_


API_TABLE_1 = 'property_visits'
API_TABLE_2 = 'page_view_and_scroll'
API_TABLE_3 = 'page_view_and_scroll_details'
API_TABLE_4 = 'subscribers'
data_kiss = get_data(API_TABLE_1)
data_kiss_2 = get_data(API_TABLE_2)
data_kiss_3 = get_data(API_TABLE_3)
data_kiss_4 = get_data(API_TABLE_4)


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
    ret.sort_values(by='minutes', ascending=False, inplace=True)
    output = lambda: ret.set_index(page)
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)


def top_entry_exit_pages():
    loc_ent = data_kiss.groupby(['Visited_from', 'EntryPage']).count()
    loc_ent = loc_ent.id.reset_index()
    loc_ent.columns = ['Visited_from', 'EntryPage', 'times_entry']
    loc_ext = data_kiss.groupby(['Visited_from', 'ExitPage']).count()
    loc_ext = loc_ext.id.reset_index()
    loc_ext.columns = ['Visited_from', 'EntryPage', 'times_exit']
    entry_exit_location = preprocess.concat(loc_ent, loc_ext.iloc[:, 1::])
    entry_exit_location.dropna(inplace=True)
    loc_info = {'country': [], 'entry_page': [], 'exit_page': []}
    locations = entry_exit_location['Visited_from'].unique()
    for each in locations:
        criteria = entry_exit_location['Visited_from'] == each
        loc_frame = entry_exit_location[criteria]
        if loc_frame.shape[0] != 0:
            entry_sort = loc_frame.sort_values(
                by='times_entry', ascending=False)
            exit_sort = loc_frame.sort_values(
                by='times_exit', ascending=False)
            loc_info['country'].append(each)
            loc_info['entry_page'].append(entry_sort.iloc[0, 1])
            loc_info['exit_page'].append(exit_sort.iloc[0, 3])
    ret = preprocess.make_frame(loc_info)
    output = lambda: ret.set_index('country')
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)


def bounce_rate(specifiers=None):
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
    match1 = data_kiss.EntryPage == 'LoginPageView'
    match2 = data_kiss.ExitPage == 'LoginPageView'
    match3 = data_kiss.minutes > 0.0
    bounced = data_kiss[match1 & match2 & match3]
    bounced_traffic = bounced.shape[0]
    total_traffic = data_kiss.shape[0]
    if not specifiers:
        bt = round(bounced_traffic / total_traffic * 100, 2)
        output = lambda: {'bounced_traffic': bt}
        if Constants.DEBUG:
            print(output())
            return True, None
        dwrap = dict_wrap(override=None)
        return True, dwrap.wrap(functor=output)
    elif specifiers == 'country':
        cs_total_traffic = data_kiss.groupby(
            'Visited_from').PropertyID.count().reset_index()
        cs_bounced_traffic = bounced.groupby(
            'Visited_from').PropertyID.count().reset_index()
        comb_traffic = cs_total_traffic.merge(
            cs_bounced_traffic, how='inner', on='Visited_from')
        bt = comb_traffic.PropertyID_y / comb_traffic.PropertyID_x * 100
        comb_traffic['BounceRate'] = bt
        comb_traffic.sort_values(
            by='BounceRate', ascending=False, inplace=True)
        ret = comb_traffic.loc[:, ['Visited_from', 'BounceRate']]
        output = lambda: ret.set_index('Visited_from')
        if Constants.DEBUG:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
        else:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)


def page_view_semantics():
    pvs = data_kiss_2.iloc[:, 1::].mean()
    pvs = pvs.sort_values(ascending=False).index.tolist()
    from collections import OrderedDict
    pages = pvs[:-1]
    rank = range(1, len(pages) + 1)
    output = lambda: OrderedDict(zip(rank, pages))
    if Constants.DEBUG:
        print(output())
        return True, None
    dwrap = dict_wrap(override=None)
    return True, dwrap.wrap(functor=output)


def conversion_path(specifiers=None):
    match1 = data_kiss_3.CalledForAction == 'Y'
    match2 = data_kiss_3.FavProperties == 'Y'
    match3 = data_kiss_3.SavedSearch == 'Y'
    converted_crowd = data_kiss_3[match1 | match2 | match3]
    if not specifiers:
        page_view_path = converted_crowd.groupby(
            'PageViews').VisitedBy.count().reset_index()
        page_view_path.sort_values('VisitedBy', ascending=False, inplace=True)
        output = lambda: page_view_path.set_index('PageViews')
        if Constants.DEBUG:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
        else:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)
    elif specifiers == 'country':
        all_page_views = data_kiss_3.PageViews.unique().tolist()
        unique_pages = set()
        for each in all_page_views:
            unique_pages = {page for page in each.split('|')}
        up = list(unique_pages)
        from copy import deepcopy
        all_path = []
        show_path = {'country': [], 'path': []}
        countries = converted_crowd.Visited_from.unique().tolist()
        paths = converted_crowd.groupby(
            ['Visited_from', 'PageViews']).VisitedBy.count().reset_index()
        for each in countries:
            country_path = paths[paths.Visited_from == each]
            if country_path.shape[0] != 0:
                show_path_1 = deepcopy(show_path)
                path = country_path.sort_values(
                    by='VisitedBy', ascending=False).iloc[0, 1]
                show_path_1['country'] = each
                show_path_1['path'] = path.split('|')
                all_path.append(show_path_1)
        ret = preprocess.make_frame(index=countries, columns=up)
        ret = ret.reset_index()
        ret = ret.rename(index=str, columns={'index': 'VisitingCountry'})
        max_pages = len(up)
        for each in all_path:
            country = each['country']
            vpath = each['path']
            temp_frame = ret[ret.VisitingCountry == country]
            for index, path in enumerate(vpath):
                temp_frame[path] = max_pages - index
            ret[ret.VisitingCountry == country] = temp_frame
        heatmap_data = preprocess.melt_frame(
            ret,
            id_vars='VisitingCountry',
            value_name='VisitPriority',
            var_name='page')
        hdp = heatmap_data.pivot(
            'VisitingCountry', 'page', 'VisitPriority')
        output = lambda: hdp[hdp.columns].astype(int)
        if Constants.DEBUG:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
        else:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)


def active_users(specifiers=None):
    online_now = data_kiss_4[data_kiss_4.OnlineNow == 'Y']
    if not specifiers:
        online_now_num = online_now.shape[0]
        active = data_kiss_4[data_kiss_4.AccountDeleted == 'N'].shape[0]
        online = {
            'percent_online': online_now_num / active * 100,
            'total_online': online_now_num}
        if Constants.DEBUG:
            print(online)
            return True, None
        else:
            dwrap = dict_wrap(override=None)
            output = lambda: online
            return True, dwrap.wrap(functor=output)
    elif specifiers == 'country':
        ret = online_now.groupby('Visited_from').OnlineNow.count()
        ret = ret.reset_index().sort_values(by='OnlineNow', ascending=False)
        output = lambda: ret.set_index('Visited_from')
        if Constants.DEBUG:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
        else:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)

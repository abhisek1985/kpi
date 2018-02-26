from .api_base import get_data
from ..constants import Constants
from six import reraise as raise_
from .api_base import preprocess
from .api_base import json_wrap, dict_wrap

API_TABLE_1 = 'property_search'
API_TABLE_2 = 'property_visits'
API_TABLE_3 = 'property_analytics'


data_search = get_data(API_TABLE_1)
data_visits = get_data(API_TABLE_2)
data_property = get_data(API_TABLE_3)


def timely_visits(typ, time_aggregation, filter_col=None):
    if typ not in ['aggregated', 'split']:
        if Constants.DEBUG:
            err_msg = 'The mentioned typ is not supported!'
            raise_(NotImplementedError, NotImplementedError(err_msg))
        return False, None

    if time_aggregation not in ['month', 'quarter']:
        if Constants.DEBUG:
            err_msg = 'Given time_aggregation is not supported'
            raise_(NotImplementedError, NotImplementedError(err_msg))
        return False, None

    data_temp = None
    data = data_visits
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

    if data_temp.shape[0] == 0:
        if Constants.DEBUG:
            print("The respective filter_col returns not result")
        dwrap = dict_wrap(override=None)
        output = lambda: {}
        return True, dwrap.wrap(functor=output)

    preprocess.extract_timeseries(
        data=data_temp,
        into=time_aggregation,
        ts_col='VisitingTimeStart',
        encode=True)
    unique_properties = data_temp['PropertyID'].unique()
    count_matrix = data_temp.groupby(
        ['PropertyID', str(time_aggregation) + '_enc'])['VisitedBy'].count()
    # print(count_matrix)
    from copy import deepcopy
    from collections import OrderedDict
    if time_aggregation == 'quarter':
        dtable = OrderedDict({
            'Quarter 1': 0,
            'Quarter 2': 0,
            'Quarter 3': 0,
            'Quarter 4': 0})
    elif time_aggregation == 'month':
        dtable = OrderedDict({
            'Jan': 0, 'Feb': 0, 'Mar': 0,
            'Apr': 0, 'May': 0, 'Jun': 0,
            'Jul': 0, 'Aug': 0, 'Sep': 0,
            'Oct': 0, 'Nov': 0, 'Dec': 0})
    ret = {}
    for each in unique_properties:
        dt = deepcopy(dtable)
        values = [count_matrix[each].get(key, 0) for key in dt]
        dt = OrderedDict(zip(dt.keys(), values))
        ret[each] = dt
    ret = preprocess.make_frame(ret)
    output = lambda: ret.transpose()[list(dtable.keys())]
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)
    # final_data = pd.DataFrame(data_dict).transpose()


def past_search_per(what, filter_col=None):
    if what not in ['Location',
                    'SearchPriceBracket',
                    'SearchKeywordsType',
                    'SearchKeywordsFeature']:
        if Constants.DEBUG:
            err_msg = 'Given what is not supported'
            raise_(AttributeError, AttributeError(err_msg))
        return False, None

    data_temp = None
    data = data_search
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

    if data_temp.shape[0] == 0:
        if Constants.DEBUG:
            print("The respective filter_col returns not result")
        dwrap = dict_wrap(override=None)
        output = lambda: {}
        return True, dwrap.wrap(functor=output)
    if what in ['Location', 'SearchPriceBracket']:
        ret = data_temp.groupby(what).VisitedBy.count().sort_values(
            ascending=False)
        output = lambda: ret.reset_index().set_index(what)
        if Constants.DEBUG:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
        else:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)
    else:
        typeSet = set()

        def _resolve_type_keys(data):
            allTypes = data.split('|')
            for each in allTypes:
                typeSet.add(each)
            return allTypes
        types = data_temp[what].apply(_resolve_type_keys)
        for each in typeSet:
            data_temp['type_' + str(each)] = 0

        ul = []
        for each in types:
            ul.append(each)
        from collections import Counter
        ret = dict(Counter(sum(ul, [])))

        output = lambda: ret
        if Constants.DEBUG:
            import sys
            dwrap = dict_wrap(override=sys.stdout)
            print(dwrap.wrap(functor=output))
            return True, None
        else:
            dwrap = dict_wrap(override=None)
            return True, dwrap.wrap(functor=output)


def search_quality(what, miss_rate=False):
    if miss_rate:
        prop_feat_list = data_property['property_features'].apply(
            lambda x: x.split('|')).values
        lut = dict(list(zip(data_property.id.values, prop_feat_list)))
        from collections import defaultdict

        def _miss_rate(lut, sp, sf, size):
            searched_res = []
            used_tags = []
            total_feature_count = defaultdict(int)
            mismatched_feature_count = defaultdict(int)
            for row in range(size):
                searched_res.append(sp[row].split('|'))
                used_tags.append(sf[row].split('|'))
            for each_sp, each_sf in zip(searched_res, used_tags):
                for each_props in each_sp:
                    actual_features = set(lut[int(each_props)])
                    searched_features = set(each_sf)
                    for each in searched_features:
                        total_feature_count[each] += 1
                    for each in searched_features - actual_features:
                        mismatched_feature_count[each] += 1
            return dict(total_feature_count), dict(mismatched_feature_count)

        actual, mismatched = _miss_rate(
            lut,
            data_search.PropertiesReturned.values,
            data_search.SearchKeywordsFeature.values,
            data_search.shape[0])
        miss_rate = defaultdict(float)
        for each in actual:
            miss_rate[each] = mismatched[each] / actual[each] * 100.0
        index = range(len(miss_rate))
        ret = preprocess.make_frame(miss_rate, index=index)
        ret = ret.iloc[0].reset_index()
        ret.columns = ['Features', 'Miss Rate']
        output = lambda: ret.sort_values(by='Miss Rate', ascending=False)
        if Constants.DEBUG:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
        else:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)

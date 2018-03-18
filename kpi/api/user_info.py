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
        target_group,
        time_aggregation='month', 
        filter_col=None):
    """
    This show count of houses viewed over time from
    the target customers mentioned by ```target_group``` and
    aggregate them over timeline mentioned by ```time_aggregation```

    ~~~~~~~~~~  Examples  ~~~~~~~~~~
    Currents:
        # Number of properties viewed each month as Buyer of all visits
        >>> historical_property_views(target_group='Buyer')

        # Number of properties viewed each week as Buyer of all visits
        >>> historical_property_views(target_group='Buyer', time_aggregation='week')

        # Number of properties viewed each quarter as Buyer of all visits
        >>> historical_property_views(target_group='Buyer', time_aggregation='quarter')

        # Number of properties viewed each month as Renter of all visits
        >>> historical_property_views(target_group='Renter')

        # Number of properties viewed each week as Renter of all visits
        >>> historical_property_views(target_group='Renter', time_aggregation='week')

        # Number of properties viewed each quarter as Renter of all visits
        >>> historical_property_views(target_group='Renter', time_aggregation='quarter')

        # Number of properties viewed each month as Renter and Buyer  of all visits
        >>> historical_property_views(target_group=['Renter', 'Buyer'])

        # Number of properties viewed each week as Renter and Buyer of all visits
        >>> historical_property_views(target_group=['Renter', 'Buyer'], time_aggregation='week')

        # Number of properties viewed each quarter as Renter and Buyer of all visits
        >>> historical_property_views(target_group=['Renter', 'Buyer'], time_aggregation='quarter')
        
        !
        # Number of properties viewed each month as Buyer of ```filter_col``` visits
        >>> historical_property_views(target_group='Buyer', filter_col=('UserID', 1))

        # Number of properties viewed each week as Buyer of ```filter_col``` visits
        >>> historical_property_views(target_group='Buyer', time_aggregation='week', filter_col=('UserID', 1))

        # Number of properties viewed each quarter as Buyer of ```filter_col``` visits
        >>> historical_property_views(target_group='Buyer', time_aggregation='quarter', filter_col=('UserID', 1))

        # Number of properties viewed each month as Renter of ```filter_col``` visits
        >>> historical_property_views(target_group='Renter', filter_col=('UserID', 1))

        # Number of properties viewed each week as Renter of ```filter_col``` visits
        >>> historical_property_views(target_group='Renter', time_aggregation='week', filter_col=('UserID', 1))

        # Number of properties viewed each quarter as Renter of ```filter_col``` visits
        >>> historical_property_views(target_group='Renter', time_aggregation='quarter', filter_col=('UserID', 1))

        # Number of properties viewed each month as Renter and Buyer  of ```filter_col``` visits
        >>> historical_property_views(target_group=['Renter', 'Buyer'], filter_col=('UserID', 1))

        # Number of properties viewed each week as Renter and Buyer of ```filter_col``` visits
        >>> historical_property_views(target_group=['Renter', 'Buyer'], time_aggregation='week', filter_col=('UserID', 1))

        # Number of properties viewed each quarter as Renter and Buyer of ```filter_col``` visits
        >>> historical_property_views(target_group=['Renter', 'Buyer'], time_aggregation='quarter', filter_col=('UserID', 1))
    Mile-stones:
        >>> for other type of ```cate

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
    if not isinstance(target_group, list):
        target_group = [target_group]
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


def call_to_action(filter_col=None):
    """
    ~~~~~~~~~~  Examples  ~~~~~~~~~~
    Currents:
        # For call to action each month of all the users
        >>> call_to_action()

        # For call to action each month of a user
        >>> call_to_action(filter_col=('UserID', 1))
    """
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
    output = lambda: ret.groupby(['month_enc', 'Action']).times.sum().reset_index()
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)


def historical_user_stats(factor, traffic_type, filter_col=None):
    """
    # Count of visits by users to a property for Buy of all visits
    >>> historical_user_stats(factor='houses', traffic_type='PropertyVisitedForBuy')

    # Count of visits by users to a property for Buy of all visits
    >>> historical_user_stats(factor='houses', traffic_type='PropertyVisitedForRent')

    # Count of visits by users from a location for Buy of all visits
    >>> historical_user_stats(factor='location', traffic_type='PropertyVisitedForBuy')

    # Count of visits by users from a location for Rent of all visits
    >>> historical_user_stats(factor='location', traffic_type='PropertyVisitedForRent')

    # Count of visits by users to a property for Buy of ```filter_col``` visits
    >>> historical_user_stats(factor='houses', traffic_type='PropertyVisitedForBuy', filter_col=('UserID', 3))

    # Count of visits by users to a property for Buy of ```filter_col``` visits
    >>> historical_user_stats(factor='houses', traffic_type='PropertyVisitedForRent', filter_col=('UserID', 3))

    # Count of visits by users from a location for Buy of ```filter_col``` visits
    >>> historical_user_stats(factor='location', traffic_type='PropertyVisitedForBuy', filter_col=('UserID', 3))

    # Count of visits by users from a location for Rent of ```filter_col``` visits
    >>> historical_user_stats(factor='location', traffic_type='PropertyVisitedForRent', filter_col=('UserID', 3))
    """
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
            'location')[traffic_type].count().reset_index()
    elif factor == 'houses':
        ret = data_temp.groupby(traffic_type).UserID.sum().reset_index()
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


def average_search_time(time_aggregation, metrics, filter_col=None, **kwargs):
    """
    This calculates the average search time for different types of
    metrics in the property search.
    >>> average_search_time(time_aggregation='month', metrics='PropertyVisitedForBuy')
    >>> average_search_time(time_aggregation='week', metrics='PropertyVisitedForBuy')
    >>> average_search_time(time_aggregation='quarter', metrics='PropertyVisitedForBuy')

    >>> average_search_time(time_aggregation='month', metrics='PropertyVisitedForRent')
    >>> average_search_time(time_aggregation='week', metrics='PropertyVisitedForRent')
    >>> average_search_time(time_aggregation='quarter', metrics='PropertyVisitedForRent')
    
    >>> average_search_time(time_aggregation='month', metrics='PropertyVisitedForBuy', other_agg='location')
    >>> average_search_time(time_aggregation='week', metrics='PropertyVisitedForBuy', other_agg='location')
    >>> average_search_time(time_aggregation='quarter', metrics='PropertyVisitedForBuy', other_agg='location')

    >>> average_search_time(time_aggregation='month', metrics='PropertyVisitedForRent', other_agg='location')
    >>> average_search_time(time_aggregation='week', metrics='PropertyVisitedForRent', other_agg='location')
    >>> average_search_time(time_aggregation='quarter', metrics='PropertyVisitedForRent', other_agg='location')

    >>> average_search_time(time_aggregation='month', metrics='PropertyVisitedForBuy', other_agg='location', filter_col=('UserID', 1))
    >>> average_search_time(time_aggregation='week', metrics='PropertyVisitedForBuy', other_agg='location', filter_col=('UserID', 1))
    >>> average_search_time(time_aggregation='quarter', metrics='PropertyVisitedForBuy', other_agg='location', filter_col=('UserID', 1))

    >>> average_search_time(time_aggregation='month', metrics='PropertyVisitedForRent', other_agg='location', filter_col=('UserID', 1))
    >>> average_search_time(time_aggregation='week', metrics='PropertyVisitedForRent', other_agg='location', filter_col=('UserID', 1))
    >>> average_search_time(time_aggregation='quarter', metrics='PropertyVisitedForRent', other_agg='location', filter_col=('UserID', 1))

    :param filter_col: As usual
    :param time_aggregation: time aggregation like month, week, quarter
    :param metrics: property for renting or property for buying
    :param kwargs:
    :return:
    """
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

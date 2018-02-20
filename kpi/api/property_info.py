"""
__author__ = "Mayukh Sarkar"
__date__ = "5th February, 2018"
__description__ = "Module is helpful to all the major APIs for
                   property listings and their related analysis
                   Please refer below the usage scenarios to see
                   the usage of different APIs. For more detailed info,
                   please follow the documentation"

__usage__ = "
    # // TODO Add later on
"
"""

from .api_base import preprocess
from .api_base import json_wrap, dict_wrap
from ..constants import Constants
from six import reraise as raise_
from .api_base import get_data
# from .api_base import GetData as get_data

API_TABLE_1 = 'property_analytics'
data = get_data(API_TABLE_1)


# @get_data(API_TABLE_1)
def number_of_properties(group, category=None, plot_type='heatmap'):
    """
    Finds number of properties for
    each category grouped by the group
    specified here.
    :param group: Feature the group the dataset
    :param category: Split category of the dataset
    :param plot: Plot type for which dataset is generated
    :return: Status and Wrapper subclass type
    """
#    data = get_data.data[API_TABLE_1]
    if group == 'price_range':
        # First check if the price_range
        # column is already present or not
        if group not in data.columns.values:
            success = preprocess.create_col(
                data=data,
                col_name=group,
                used_cols=['listing_price'],
                operation='CREATE_RANGE',
                interval=1000000
            )
            if not success:
                if Constants.DEBUG:
                    print('Create Column operation was not successful')
                return False, None
    # Check if the category is already present or not
    else:
        if Constants.DEBUG:
            msg = 'Bad mentioned group {}. Try using price_range'.format(group)
            raise_(ValueError, ValueError(msg))
        return False, None
    if category not in data.columns.values:
        if Constants.DEBUG:
            print('Category column is not present in the table')
        return False, None

    # Currently only 'heatmap' is supported
    if plot_type == 'heatmap':
        searches = data.groupby([group, category]).count().reset_index()
        search_data = searches.iloc[:, 0:3]
        search_data.columns = ['Price Range', 'Locations', 'count']
        output = lambda: search_data.pivot(
            'Locations',
            'Price Range',
            'count').fillna(0).astype(int)
        # Wrap into a wrapper class and return with status

        # Use file to dump the data
        # fil = open('test.json', 'w')
        # jwrap = json_wrap(override=fil)

        # Return data for API
        if not Constants.DEBUG:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)
        # pretty print data for debug
        else:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
    else:
        if Constants.DEBUG:
            print('Plot type {} is not supported'.format(plot_type))
        return False, None


# @get_data(API_TABLE_1)
def visitor_stats(n, typ, filter_col=None, plot_type='bar'):
    """
    This first filter the data according to the ```filter``` column
    and then find the individual property statistics of type ```typ```
    for top ```n``` values to yield the plot type of ```plot_type```.

    :param n: number of top values -> int
    :param typ: Type of aggregation (Unique, leads) -> str
    :param filter_col: group of values (column and value) -> tuple
    :param plot_type: Type of the plot
    :return: Status and Wrapper subclass type
    """
#    data = get_data.data[API_TABLE_1]
    data_temp = None
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

    if typ.lower() not in ['unique', 'leads']:
        if Constants.DEBUG:
            msg = "{} is a invalid aggregate function".format(typ)
            raise_(ValueError, ValueError(msg))
        else:
            return False, None
    else:
        typ = typ.lower()

    if data_temp is None:
        data_temp = data.copy(deep=True)

    if typ == 'unique':
        data_temp.sort_values(
            by='searched_by_user',
            inplace=True,
            ascending=False)
        data_temp.reset_index(inplace=True, drop=True)
        data_temp = data_temp[['id', 'searched_by_user']]
        data_temp.columns = ['property_id', 'unique_visits']
    elif typ == 'leads':
        data_temp.sort_values(
            by='user_taken_action',
            inplace=True,
            ascending=False)
        data_temp.reset_index(inplace=True, drop=True)
        data_temp = data_temp[['id', 'user_taken_action']]
        data_temp.columns = ['property_id', 'user_actions']

    if n > data_temp.shape[0] or n < 0:
        if Constants.DEBUG:
            msg = "{} is a invalid number of rows to return".format(n)
            raise_(ValueError, ValueError(msg))
        else:
            return False, None
    else:
        data_temp = data_temp.head(n=n)

    if data_temp.shape[0] == 0:
        if Constants.DEBUG:
            print('The return operation has no entries')
        dwrap = dict_wrap(override=None)
        output = lambda: {}
        return True, dwrap.wrap(functor=output)

    if plot_type == 'bar':
        data_temp.set_index('property_id', inplace=True)
        output = lambda: data_temp.reset_index(drop=True)
    else:
        if Constants.DEBUG:
            print('Plotting type {} is not supported'.format(plot_type))
        return False, None
    # Wrap into a wrapper class and return with status

    # Use file to dump the data
    # fil = open('test.json', 'w')
    # jwrap = json_wrap(override=fil)

    # Return data for API
    if not Constants.DEBUG:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)
    else:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    # pretty print data for debug
    # import sys
    # jwrap = json_wrap(override=sys.stdout)


# @get_data(API_TABLE_1)
def property_price_stats(percents, plot_type='bar'):
    """
    This calculates the percentages of the properties
    that are sold below the bank prices. There must be
    atleast two values of percentages present. It works
    like the following way.

    For percents = [20, 50]

        It calculates
            1. Properties being sold below 20% bank value
            2. Propeties being sold between 20% to 50% bank value
            3. Properties being sold above 50% the bank value

    :param percents: percentage values -> list
    :param plot_type: Plot type -> str
    """
#    data = get_data.data[API_TABLE_1]
    if len(percents) < 2:
        if Constants.DEBUG:
            msg = 'Minimum 2 percentages should be given'
            raise_(AttributeError, AttributeError(msg))
        return False, None

    if plot_type == 'bar':
        if preprocess.get_percentage(
                data,
                'bank_price',
                'listing_price',
                'sold_below_bank'):
            data_temp = data[data['sold_below_bank'] >= 0].copy(deep=True)
            price_counts = dict()
            low = percents[0]
            high = percents[-1]
            sbb = data_temp['sold_below_bank']
            price_below = data_temp[sbb <= low].shape[0]
            price_above = data_temp[sbb >= high].shape[0]
            for i in range(len(percents) - 1):
                curr = percents[i]
                nxt = percents[i + 1]
                key = 'price_' + str(curr) + '_to_' + str(nxt)
                low_cond = data_temp['sold_below_bank'] >= curr
                high_cond = data_temp['sold_below_bank'] <= nxt
                price_counts[key] = data_temp[low_cond & high_cond].shape[0]
            price_counts['price_below_' + str(low)] = price_below
            price_counts['price_above_' + str(high)] = price_above
            if not Constants.DEBUG:
                dwrap = dict_wrap(override=None)
                output = lambda: price_counts
                return True, dwrap.wrap(functor=output)
            else:
                print(price_counts)
                return True, None
    else:
        if not Constants.DEBUG:
            dwrap = dict_wrap(override=None)
            output = lambda: {}
            return True, dwrap.wrap(functor=output)
        else:
            print({})
            return True, None


# @get_data(API_TABLE_1)
def national_price_tally(
        n,
        national_price,
        filter_col=None,
        plot_type='bar'):
    """
    Compares the price of properties to check the top ```n``` above or
    below average prices compared to the national_prices based on the
    filter_type to screen for a filter.

    :param plot_type: Plotting type
    :param n: number of properties above or below NP
    :param national_price: The national_price of the location
    :param filter_col: group of values (column and value) -> tuple
    """
#    data = get_data.data[API_TABLE_1]

    # This is to see if below NP or above NP properties are taken
    price_above_na = True
    if n < 0:
        price_above_na = False
    data_temp = None
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
    preprocess.generic_operations(
        data_temp,
        'listing_price',
        'size',
        'square_meter_price',
        '/')
    data_temp['more_than_NA'] = data_temp['square_meter_price'].apply(
        lambda x: x > national_price)
    preprocess.generic_operations(
        data_temp,
        'square_meter_price',
        national_price,
        'per_below_above_NA_temp',
        '-')
    preprocess.generic_operations(
        data_temp,
        'per_below_above_NA_temp',
        national_price * 100,
        'per_below_above_NA',
        '/')
    data_temp.drop(
        ['per_below_above_NA_temp'],
        axis=1,
        inplace=True)

    if price_above_na:
        ret_data = data_temp[data_temp['more_than_NA'] == 1]
    else:
        ret_data = data_temp[data_temp['more_than_NA'] == 0]

    if ret_data.shape[0] == 0:
        if Constants.DEBUG:
            print('Operation returned no values')
        dwrap = dict_wrap(override=None)
        output = lambda: {}
        return True, dwrap.wrap(functor=output)

    if abs(n) > ret_data.shape[0]:
        if Constants.DEBUG:
            msg = "{} is a invalid number of rows to return".format(n)
            raise_(ValueError, ValueError(msg))
        else:
            return False, None
    else:
        ascend = not price_above_na
        ret_data.sort_values(
            by='per_below_above_NA',
            inplace=True,
            ascending=ascend)
        ret_data = ret_data[['id', 'per_below_above_NA']].head(n)

        if plot_type == 'bar':
            ret_data.set_index('id', inplace=True)
            output = lambda: ret_data.reset_index(drop=True)
        else:
            if Constants.DEBUG:
                print('Plot type {} is not supported'.format(plot_type))
            return False, None
        if not Constants.DEBUG:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)
        else:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None


# @get_data(API_TABLE_1)
def property_discounts(
        n,
        filter_col=None,
        typ='aggregated',
        focus='location',
        plot_type='bar'):
    """
    Calculates the discounts offered by for different properties
    and returns the top ```n``` properties. It also can take
    ```filter_col``` as a tuple and filter the data according
    to the mentioned column and given value and then perform the
    discount calculations. This can be used to filter seller specific
    data.

    It supports the discount analysis focused on either **property_id**
    or on **location** as mentioned by ```focus```.

    For focus on location, it can either performed **aggregated** per location
    analysis or **individual** discount oriented analysis as
    mentioned by ```typ```.

    :param n: Top n values. -> int
    :param filter_col: Filtering criteria -> tuple
    :param typ: Analysis type for focus on location -> str
    :param focus: Focus for either location or property -> str
    :param plot_type: Plotting type data munging -> str
    """
#    data = get_data.data[API_TABLE_1]
    if typ not in ['aggregated', 'individual']:
        if Constants.DEBUG:
            msg = '{} type is not implemented yet'.format(typ)
            raise_(NotImplementedError, NotImplementedError(msg))
        return False, None

    if focus not in ['location', 'property']:
        if Constants.DEBUG:
            msg = '{} focus is not implemented'.format(focus)
            raise_(NotImplementedError, NotImplementedError(msg))
        return False, None

    if n <= 0:
        if Constants.DEBUG:
            msg = '{} n value is not valid'.format(n)
            raise_(ValueError, ValueError(msg))
        return False, None
    data_temp = None
    if filter_col and len(filter_col) == 2:
        try:
            fc = filter_col[0]
            fv = filter_col[1]
            data_temp = data[data[fc] == fv].copy(deep=True)
        except KeyError:
            if Constants.DEBUG:
                msg = 'filter column {} is not present'.format(filter_col[0])
                raise_(AttributeError, AttributeError(msg))
            return False, None

    if data_temp is None:
        data_temp = data.copy(deep=True)

    if focus == 'location':
        preprocess.generic_operations(
            data_temp,
            'bank_price',
            'listing_price',
            'Sale',
            '-')
        if typ == 'aggregated':
            data_temp['Sale'] = data_temp['Sale'].apply(
                lambda x: 0 if x < 0 else x / data_temp.Sale.max())
            discount = data_temp.groupby('location').Sale.sum()
            discount.sort_values(ascending=False, inplace=True)
            data_temp = discount.reset_index()
            data_temp.columns = ['Location', 'Discount Rate']
            data_temp = data_temp.reset_index(drop=True).set_index('Location')
        elif typ == 'individual':
            data_temp['Sale'] = data_temp['Sale'].apply(
                lambda x: 0 if x < 0 else x)
            data_temp = data_temp[['id', 'location', 'Sale']]
            data_temp.sort_values(by='Sale', ascending=False, inplace=True)
            data_temp.columns = ['property_id', 'Location', 'Discount Price']
            data_temp = data_temp.reset_index(drop=True).set_index(
                'property_id')
        else:
            if Constants.DEBUG:
                msg = 'Type {} is not implemented'.format(typ)
                raise_(NotImplementedError, NotImplementedError(msg))
            return False, None
    elif focus == 'property':
        data_temp = data_temp[data_temp.bank_certification == 1]
        preprocess.generic_operations(
            data_temp,
            'bank_price',
            'listing_price',
            'price_gap',
            '-')
        data_temp.sort_values('price_gap', ascending=False, inplace=True)
        data_temp = data_temp[['id', 'price_gap']]
        data_temp = data_temp.reset_index(drop=True).set_index('id')
    else:
        if Constants.DEBUG:
            msg = 'Focus {} is not supported yet'.format(focus)
            raise_(NotImplementedError, NotImplementedError(msg))
        return False, None

    if data_temp.shape[0] == 0:
        if Constants.DEBUG:
            print('Operation returned no data')
        dwrap = dict_wrap(override=None)
        output = lambda: {}
        return True, dwrap.wrap(functor=output)
    elif data_temp.shape[0] < n:
        if Constants.DEBUG:
            msg = '{} n is too high'.format(n)
            raise_(AttributeError, AttributeError(msg))
        return False, None

    if plot_type == 'bar':
        output = lambda: data_temp.head(n=n)
        if Constants.DEBUG:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
        else:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)
    else:
        if Constants.DEBUG:
            msg = 'Plot type {} is not supported yet'.format(plot_type)
            raise_(NotImplementedError, NotImplementedError(msg))
        return False, None

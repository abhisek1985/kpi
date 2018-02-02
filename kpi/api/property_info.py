from .api_base import preprocess
from .api_base import API
from .api_base import json_wrap, dict_wrap
from six import reraise as raise_


data = API(base='django')['property_analytics']  # pandas.DataFrame


def number_of_properties(group, category=None, plot='heatmap'):
    """
    Finds number of properties for
    each category grouped by the group
    specified here.
    :param group: Feature the group the dataset
    :param category: Split category of the dataset
    :param plot: Plot type for which dataset is generated
    :return: Status and Wrapper subclass type
    """
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
                return False, None
    # Check if the category is already present or not
    if category not in data.columns.values:
        return False, None

    # Currently only 'heatmap' is supported
    if plot == 'heatmap':
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
        # jwrap = json_wrap(override=None)

        # pretty print data for debug
        import sys
        jwrap = json_wrap(override=sys.stdout)
        return True, jwrap.wrap(functor=output, indent=4)


def visitor_stats(n, typ, filter_col=None, plot_type='bar', **kwargs):
    """
    This first filter the data according to the ```filter``` column
    and then find the indivisual property statistics of type ```typ```
    for top ```n``` values to yield the plot type of ```plot_type```.

    :param n: number of top values -> int
    :param typ: Type of aggregation (Unique, leads) -> str
    :param filter_col: group of values (column and value) -> tuple
    :param plot_type: Type of the plot
    :return: Status and Wrapper subclass type
    """
    if filter_col and len(filter_col) == 2:
        try:
            fc = filter_col[0]
            fv = filter_col[1]
            data_temp = data[data[fc] == fv].copy(deep=True)
        except KeyError:
            raise AttributeError('filter column is not present')
    else:
        data_temp = data.copy(deep=True)

    if typ.lower() not in ['unique', 'leads']:
        msg = "{} is a invalid aggregate function".format(typ)
        raise_(ValueError, ValueError(msg))
        # raise ValueError("{} is a invalid aggregate function".format(typ))
    else:
        typ = typ.lower()

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
        msg = "{} is a invalid number of rows to return".format(n)
        raise_(ValueError, ValueError(msg))
    else:
        data_temp = data_temp.head(n=n)

    if data_temp.shape[0] == 0:
        return False, None

    if plot_type == 'bar':
        data_temp.set_index('property_id', inplace=True)
        output = lambda: data_temp.reset_index(drop=True)
    else:
        return False, None
    # Wrap into a wrapper class and return with status

    # Use file to dump the data
    # fil = open('test.json', 'w')
    # jwrap = json_wrap(override=fil)

    # Return data for API
    # jwrap = json_wrap(override=None)

    # pretty print data for debug
    import sys
    jwrap = json_wrap(override=sys.stdout)
    return True, jwrap.wrap(functor=output, indent=4)


def property_price_stats(percents, plot_type='bar'):
    """
    # // TODO __doc__ later on
    """
    if len(percents) < 2:
        return False, None

    if preprocess.get_percentage(
        data,
        'bank_price',
        'listing_price',
        'sold_below_bank'):
        data_temp = data[data['sold_below_bank'] >= 0].copy(deep=True)
        price_counts = dict()
        low = percents[0]
        high = percents[-1]
        price_below = data_temp[data_temp['sold_below_bank'] <= low].shape[0]
        price_above = data_temp[data_temp['sold_below_bank'] >= high].shape[0]
        for i in range(len(percents) - 1):
            curr = percents[i]
            nxt = percents[i + 1]
            key = 'price_' + str(curr) + '_to_' + str(nxt)
            low_cond = data_temp['sold_below_bank'] >= curr
            high_cond = data_temp['sold_below_bank'] <= nxt
            price_counts[key] = data_temp[low_cond & high_cond].shape[0]
        price_counts['price_below_' + str(low)] = price_below
        price_counts['price_above_' + str(high)] = price_above
        dwrap = dict_wrap(override=None)
        output = lambda: price_counts
        return True, dwrap.wrap(functor=output)


def national_price_tally(n, national_price, filter_col=None, plot_type='bar', **kwargs):
    """
    Compares the price of properties to check the top ```n``` above or
    below average prices compated to the national_prices based on the
    filter_type to screen for a filter.

    :param n: number of propeties above or below NP
    :param national_prices: The national_price of the location
    :param filter_col: group of values (column and value) -> tuple
    """

    # This is to see if below NP or above NP properties are taken
    more_than_np = False
    less_than_np = False
    if n < 0:
        less_than_np = True
    else:
        more_than_np = True

    if filter_col and len(filter_col) == 2:
        try:
            fc = filter_col[0]
            fv = filter_col[1]
            data_temp = data[data[fc] == fv].copy(deep=True)
        except KeyError:
            raise AttributeError('filter column is not present')
    else:
        data_temp = data.copy(deep=True)
    preprocess.generic_operations(
        data_temp,
        'listing_price',
        'size',
        'square_meter_price',
        '/')
    print(data_temp.head())

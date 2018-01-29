from .api_base import preprocess
from .api_base import API
from .api_base import json_wrap
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
    :return: Boolean (SUCCESS or FAILURE)
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


def visitor_stats(n, filter_col=None, typ='unique', plot_type='bar'):
    """
    This first filter the data according to the ```filter``` column
    and then find visitor to properties statistics of type ```typ```
    for top ```n``` values to yield the plot type of ```plot_type```.

    :param n: number of top values -> int
    :param filter_col: group of values for the col and particular value -> tuple
    :param typ: Type of aggregation (Unique, total, mode, max, min)
    :param plot_type: Type of the plot
    :return: Wrapper subclass type
    """
    if filter_col and len(filter_col) == 2:
        try:
            data_temp = data[data[filter_col[0]] == filter_col[1]]
        except KeyError:
            raise AttributeError('filter column is not present')
    else:
        data_temp = data

    if typ.lower() not in ['unique', 'total', 'mode', 'max', 'min']:
        msg = "{} is a invalid aggregate function".format(typ)
        raise_(ValueError, ValueError(msg))
        # raise ValueError("{} is a invalid aggregate function".format(typ))
    if typ == 'unique':
        data_temp = data_temp.groupby(by='searched_by_user').count().reset_index()
        data_temp = data_temp[['searched_by_user', 'id']]
        data_temp.columns = ['seller_id', 'count']
        data_temp.sort_values(by='count', ascending=False, inplace=True)

    if n > data_temp.shape[0] or n < 0:
        msg = "{} is a invalid number of rows to return".format(n)
        raise_(ValueError, ValueError(msg))
        # raise ValueError("{} is a invalid number of rows to return".format(n))
    else:
        data_temp = data_temp.head(n=n)
    output = lambda: data_temp.reset_index(drop=True)
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

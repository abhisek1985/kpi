from .api_base import get_data
from ..constants import Constants
from six import reraise as raise_
from .api_base import json_wrap, dict_wrap

API_TABLE_1 = 'price_burndown'

data_invest = get_data(API_TABLE_1)


def get_investment_hotspots(location, n, typ='N'):
    """
    Get the top investment spots in the mentioned
    ```location``` for last ```n``` months only
    restricted to the properties which are of type
    mentioned by ```typ```.

    :param location: Location mentioned
    :param n: Count of last n months
    :param typ: Type of property
    """
    if typ not in ['Y', 'N', 'P']:
        if Constants.DEBUG:
            err_msg = 'Mentioned typ is not valid'
            raise_(AttributeError, AttributeError(err_msg))
        return False, None
    if n < 2 or n > 12:
        if Constants.DEBUG:
            err_msg = 'n value should be in range of 2 to 12'
            raise_(ValueError, ValueError(err_msg))
        return False, None

    def _monotonic(row):
        col_list = [
            'priceOnMonth1',
            'priceOnMonth2',
            'priceOnMonth3',
            'priceOnMonth4',
            'priceOnMonth5',
            'priceOnMonth6',
            'priceOnMonth7',
            'priceOnMonth8',
            'priceOnMonth9',
            'priceOnMonth10',
            'priceOnMonth11',
            'priceOnMonth12']
        k = row[col_list]
        k.dropna(inplace=True)
        investment_graded = []
        for i in range(len(k)):
            temp = k[i: i + n]
            if len(temp) == n:
                investment_graded.append(temp.is_monotonic_increasing)
            else:
                return False
        return all(investment_graded[-n:])
    mask1 = data_invest.City == location
    mask2 = data_invest.Completed == typ
    data_loc = data_invest[mask1 & mask2].copy()
    data_loc['invest_hotspot'] = data_loc.apply(_monotonic, axis=1)
    ret = data_loc[data_loc.invest_hotspot == 1].PropertyId.tolist()
    output = lambda: {'investment_hotspot': ret}
    if Constants.DEBUG:
        import sys
        dwrap = dict_wrap(override=sys.stdout)
        print(dwrap.wrap(functor=output))
        return True, None
    else:
        dwrap = dict_wrap(override=None)
        return True, dwrap.wrap(functor=output)


def slashed_prices(location, n, **kwargs):
    """
    Shows the top ```n``` properties
    whose per-square feet price slashed
    most from the last month at ```location```.

    :param location: Location for property search
    :param n: Top n properties
    """
    def _psft_price_change(row):
        col_list = [
            'priceOnMonth1',
            'priceOnMonth2',
            'priceOnMonth3',
            'priceOnMonth4',
            'priceOnMonth5',
            'priceOnMonth6',
            'priceOnMonth7',
            'priceOnMonth8',
            'priceOnMonth9',
            'priceOnMonth10',
            'priceOnMonth11',
            'priceOnMonth12']
        k = row[col_list]
        k.dropna(inplace=True)
        if len(k) < 2:
            return -1
        else:
            psfp = k / row.Size
            return (psfp[-1] - psfp[-2]) / psfp[-2] * 100

    data_loc = data_invest[data_invest.City == location].copy()
    data_loc['percent_increase'] = data_loc.apply(_psft_price_change, axis=1)
    ret = data_loc.sort_values(by='percent_increase', ascending=False)
    completed = kwargs.get('completed', False)
    if completed:
        ret = ret[ret.Completed == 'Y']
    ret = ret.loc[:, ['PropertyId', 'percent_increase']].reset_index()
    output = lambda: ret.head(n).set_index('PropertyId')
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)

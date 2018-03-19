from .api_base import get_data
from .api_base import json_wrap, dict_wrap
from ..constants import Constants
from six import reraise as raise_


API_TABLE_1 = 'subscribers'
data = get_data(API_TABLE_1)

# ###################### Private method ####################


def _assign_major_role(row):
    maximum_time_spent_as = max(
        [
            row['AsBuyer'],
            row['AsFSBO'],
            row['AsInvestor'],
            row['AsPropMngr'],
            row['AsAgent']
        ])
    if maximum_time_spent_as == row['AsBuyer']:
        return 'Buyer'
    elif maximum_time_spent_as == row['AsFSBO']:
        return 'Seller'
    elif maximum_time_spent_as == row['AsInvestor']:
        return 'Investor'
    elif maximum_time_spent_as == row['AsAgent']:
        return 'Agent'
    else:
        return 'Property_Manager'
###########################################################


def active_users(specifiers=None, user_group='*'):
    """
    Gets all the active users for the user groups split
    by the specifiers mentioned
    >>> karakira_admin.active_users(user_group='Property_Manager',
                                    specifiers='country')
    >>> karakira_admin.active_users(user_group='Agent')
    >>> karakira_admin.active_users()
    >>> karakira_admin.active_users(specifiers='country')

    :param specifiers: The specifier to split the aggregation
    :param user_group: For whom the analysis needs to be done
    """
    user_groups = [
        '*', 'Buyer', 'Seller', 'Investor',
        'Agent', 'Property_Manager'

    ]

    if user_group not in user_groups:
        if Constants.DEBUG:
            err_msg = 'Mentioned user group {} is invalid'.format(user_group)
            raise_(AttributeError, ArithmeticError(err_msg))
        return False, None
    elif user_group != '*':
        data['Role'] = data.apply(_assign_major_role, axis=1)
        data_active = data[data.Role == user_group].copy()
    else:
        data_active = data.copy()

    online_now = data_active[data_active.OnlineNow == 'Y']
    if not specifiers:
        online_now_num = online_now.shape[0]
        active = data_active[data_active.AccountDeleted == 'N'].shape[0]
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
    else:
        if Constants.DEBUG:
            err_msg = 'Mentioned specifier {} is wrong!!'.format(specifiers)
            raise_(AttributeError, AttributeError(err_msg))
        return False, None


def cancelled_users(specifiers=None, user_group='*'):
    user_groups = [
        '*', 'Buyer', 'Seller', 'Investor',
        'Agent', 'Property_Manager'

    ]

    if user_group not in user_groups:
        if Constants.DEBUG:
            err_msg = 'Mentioned user group {} is invalid'.format(user_group)
            raise_(AttributeError, ArithmeticError(err_msg))
        return False, None
    elif user_group != '*':
        data['Role'] = data.apply(_assign_major_role, axis=1)
        data_active = data[data.Role == user_group].copy()
    else:
        data_active = data.copy()
    pass

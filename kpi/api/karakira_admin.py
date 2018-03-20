from .api_base import get_data
from .api_base import json_wrap, dict_wrap
from ..constants import Constants
from .api_base import preprocess
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


def get_online_subscribers(specifier=None):
    if specifier and specifier not in data.columns.values:
        if Constants.DEBUG:
            err_msg = 'Mentioned specifier is not present'
            raise_(AttributeError, AttributeError(err_msg))
    if not specifier:
        ret = data[data.OnlineNow == 'Y'].SubscriberID.unique().tolist()
        ret = {'online_subscribers': ret}
        output = lambda: ret
        if Constants.DEBUG:
            print(output())
            return True, None
        else:
            dwrap = dict_wrap(override=None)
            return True, dwrap.wrap(functor=output)
    else:
        specifier_subscriber = {}
        for grp, name in data[data.OnlineNow == 'Y'].groupby(specifier):
            specifier_subscriber[grp] = name.SubscriberID.unique().tolist()
        output = lambda: specifier_subscriber
        if Constants.DEBUG:
            print(output())
            return True, None
        else:
            dwrap = dict_wrap(override=None)
            return True, dwrap.wrap(functor=output)


def top_n_agents(n):
    data['Role'] = data.apply(_assign_major_role, axis=1)
    agents = data[data.Role == 'Agent']
    agents = agents[agents.AccountDeleted == 'N'].copy()
    ret = agents.groupby('SubscriberID').AsAgent.mean()
    ret = ret.reset_index()
    ret.columns = ['AgentID', 'score']
    ret = ret.sort_values(by='score', ascending=False).head(n)
    output = lambda: ret.set_index('AgentID')
    if Constants.DEBUG:
        import sys
        jwrap = json_wrap(override=sys.stdout)
        print(jwrap.wrap(functor=output, indent=4))
        return True, None
    else:
        jwrap = json_wrap(override=None)
        return True, jwrap.wrap(functor=output, indent=4)


def cancelled_users(spec1=None, spec2='Location', user_group='*', **kwargs):
    user_groups = [
        '*', 'Buyer', 'Seller', 'Investor',
        'Agent', 'Property_Manager'
    ]
    data['Role'] = data.apply(_assign_major_role, axis=1)
    if user_group not in user_groups:
        if Constants.DEBUG:
            err_msg = 'Mentioned user group {} is invalid'.format(user_group)
            raise_(AttributeError, ArithmeticError(err_msg))
        return False, None
    elif user_group != '*':
        data_user = data[data.Role == user_group].copy()
    else:
        data_user = data.copy()

    data_user = data_user[data_user.AccountDeleted == 'Y']
    if not spec1:
        ret = data_user.groupby(spec2).AccountDeleted.count()
        output = lambda: ret.reset_index().set_index(spec2)
        if Constants.DEBUG:
            import sys
            jwrap = json_wrap(override=sys.stdout)
            print(jwrap.wrap(functor=output, indent=4))
            return True, None
        else:
            jwrap = json_wrap(override=None)
            return True, jwrap.wrap(functor=output, indent=4)
    else:
        if spec1 == 'Role':
            ret = data_user.groupby([spec2, spec1]).size().reset_index()
            ret.columns = ['Location', 'Role', 'Cancelled']
            ret = ret.pivot('Location', 'Role', 'Cancelled')
            ret.fillna(0, inplace=True)
            output = lambda: ret.astype(int)
            if Constants.DEBUG:
                import sys
                jwrap = json_wrap(override=sys.stdout)
                print(jwrap.wrap(functor=output, indent=4))
                return True, None
            else:
                jwrap = json_wrap(override=None)
                return True, jwrap.wrap(functor=output, indent=4)
        elif spec1 == 'time':
            tagg = kwargs.get('time_aggregation', 'month')
            preprocess.extract_timeseries(
                data=data_user,
                ts_col='JoiningDate')
            preprocess.extract_timeseries(
                data=data_user,
                into=tagg,
                encode=True,
                ts_col='JoiningDate')
            ret = data_user.groupby(
                [spec2, str(tagg) + '_enc']).size().reset_index()
            cols = ['Location', 'Cancel_' + str(tagg), 'Count']
            ret.columns = cols
            ret = ret.pivot('Location', 'Cancel_' + str(tagg), 'Count')
            ret.fillna(0, inplace=True)
            output = lambda: ret.astype(int)
            if Constants.DEBUG:
                import sys
                jwrap = json_wrap(override=sys.stdout)
                print(jwrap.wrap(functor=output, indent=4))
                return True, None
            else:
                jwrap = json_wrap(override=None)
                return True, jwrap.wrap(functor=output, indent=4)

from .api_base import get_data


API_TABLE_1 = 'user_info'
API_TABLE_2 = 'user_info_detailed'

data_user_info = get_data(API_TABLE_1)
data_user_info_det = get_data(API_TABLE_2)


def test():
    print(data_user_info)

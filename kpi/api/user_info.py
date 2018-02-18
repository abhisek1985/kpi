from .api_base import GetData as get_data
from .api_base import data


API_TABLE_1 = 'user_info'
API_TABLE_2 = 'user_info_detailed'


@get_data(API_TABLE_1)
def test():
    print(data)
    # print(data[API_TABLE_1].head())

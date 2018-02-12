from .api_base import GetData as get_data

API_TABLE_1 = 'user_info'


@get_data(API_TABLE_1)
def test():
    print(get_data.data[API_TABLE_1].head())

from .api_base import API
data = API(base='django')['user_info']

print(data.head())
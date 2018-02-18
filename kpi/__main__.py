# from kpi import dispatch
# from kpi.constants import Constants
from kpi import user_info

#
# if __name__ == '__main__':
#     dbuf = DBBuffer()
#     print(dbuf['property_analytics'])


def main():
    user_info.test()
    # dbuf = dispatch.DBBuffer()
    # if Constants.DEBUG:
    #     print(dbuf['property_analytics'])
    # else:
    #     print('Debug mode is off')


if __name__ == '__main__':
    main()

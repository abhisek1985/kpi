from kpi import dispatch
from kpi.constants import Constants


#
# if __name__ == '__main__':
#     dbuf = DBBuffer()
#     print(dbuf['property_analytics'])

def main():
    dbuf = dispatch.DBBuffer()
    if Constants.DEBUG:
        print(dbuf['property_analytics'])
    else:
        print('Debug mode is off')

if __name__ == '__main__':
    main()
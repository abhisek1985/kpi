from kpi import dispatch

#
# if __name__ == '__main__':
#     dbuf = DBBuffer()
#     print(dbuf['property_analytics'])

def main():
    dbuf = dispatch.DBBuffer()
    print(dbuf['property_analytics'])
    # from kpi.constants import DEBUG
    # print(DEBUG)
    # if DEBUG:
    #     print(dbuf['property_analytics'])
    # else:
    #     print('Debug mode is off')

if __name__ == '__main__':
    main()
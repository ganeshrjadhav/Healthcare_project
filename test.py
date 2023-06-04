date = '01JUL2021'
def convert_date(date):
    months = {'JAN': '01','FEB':'02','MAR':'03','JUL':'07','APR':'04','MAY':'05','JUN':'06','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'}

    datelist = list(date)
    datelist.insert(2,'-')
    datelist.insert(6,'-')
    #print(datelist)
    #print(type(datelist))
    date = ''.join(datelist)

    lst = date.split('-')
    #print(lst)

    for i in lst:
        if i in months.keys():
            lst[lst.index(i)] = months[i]

    date ='-'.join(lst)

    return (date)

convert_date(date)
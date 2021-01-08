with open('/Users/harper/ssb/lineorder.tbl') as lineorder:
    sum = 0
    for line in lineorder.read().splitlines():
        columns = line.split('|')
        discount = int(columns[11])
        quantity = int(columns[8])
        orderdate = columns[5]
        extendedprice = float(columns[9])
        if discount >= 1 and discount <= 3 and quantity < 25 and orderdate >= '19940101' and orderdate <= '19941231':
            sum += extendedprice * discount
    print(sum)
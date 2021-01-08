with open('lineorder.tbl') as lineorder:
    for line in lineorder.read().splitlines():
        columns = line.splitlines('|')
        discount = int(columns(11))
        quantity = int(columns(8))
        orderdate = columns(5)
        extendedprice = float(columns(9))
        sum = 0
        if discount >= 1 and discount <= 3 and quantity < 25 and orderdate >= '19940101' and orderdate <= '19941231':
            sum += extendedprice * discount
    print(sum)
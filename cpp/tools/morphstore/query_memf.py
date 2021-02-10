import sys

NUM_COLS = 7

dict = {}
with open(sys.argv[1]) as f:
    lines = f.read().splitlines()
    for line in lines:
        fields = line.split('\t')
        if len(fields) == NUM_COLS:
            value = fields[NUM_COLS - 1]
            if value.isnumeric():
                key = fields[0]
                size = int(value)
                if size == 0:
                    size = int(fields[NUM_COLS - 2])
                if key in dict:
                    dict[key] = min(dict[key],size)
                else:
                    dict[key] = size
sum = 0
for key in dict:
    if key.find(".") == -1:
        sum+= dict[key]
print(sum)

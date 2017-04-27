import MapReduce
import sys

"""
Word Count Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[1]
    value = record[:]
    #words = key.split()
    #for w in words:
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    all_values = [order + line_item
                  for line_item in list_of_values
                  for order in list_of_values if
                  order[0] == "order" and line_item[0] == 'line_item']


    #for o in orders:
    #    for l in line_items:
    #        all_values.append(o + l)

    for v in all_values:
        mr.emit((v))



# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

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

    # A = 5x5; B = 5x5; AXB = 5x5
    MATRIX_OUTPUT = (5,5)


    if record[0] == 'a':
        key = record[1]
        value = (record[2], record[3])
        for i in range(MATRIX_OUTPUT[1]):
            mr.emit_intermediate((key, i), value)
    if record[0] == 'b':
        key = record[2]
        value = (record[1], record[3])
        for i in range(MATRIX_OUTPUT[0]):
            mr.emit_intermediate((i, key), value)



#http://hadoopgeek.com/mapreduce-matrix-multiplication/


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    A_COLS = 5
    def getCells(mat, **kwargs):
        cellList = mat
        if 'row' in kwargs:
            cellList = [x for x in cellList if cellList[1] == kwargs['row']]
        if 'col' in kwargs:
            cellList = [x for x in cellList if cellList[1] == kwargs['col']]

        return [x[2] for x in cellList]

    def cross_prod(i):
        cross = [x[1] for x in list_of_values if x[0] == i]
        try:
            return cross[0]*cross[1]
        except IndexError:
            return 0

    mr.emit((key[0], key[1], sum([cross_prod(x) for x in range(A_COLS)])))





# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
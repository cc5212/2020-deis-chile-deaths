import csv

with open('./data/DEF_1990-2017.csv','r') as csvin, open('./data/deis.tsv', 'w') as tsvout:
    csvin = csv.reader(csvin, delimiter=";")
    tsvout = csv.writer(tsvout, delimiter='\t', lineterminator="\n")
    next(csvin)

    index = [0,3,4,6,11,13,15,18,22,27,29,33,38,42]

    for row in csvin:
        tsvout.writerow((row[0], row[3], row[4], row[6],
                         row[11], row[13], row[15], row[18],
                         row[22], row[27], row[29], row[33],
                         row[38], row[42]))
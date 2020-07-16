import csv

with open('./data/DEF_1990-2017.csv','r') as csvin, open('./data/deis-data.tsv', 'w', encoding='utf-8') as tsvout:
    csvin = csv.reader(csvin, delimiter=";")
    tsvout = csv.writer(tsvout, delimiter='\t', lineterminator="\n")
    next(csvin)

    for row in csvin:
        tsvout.writerow((row[0], row[3], row[4], row[6],
                         row[11], row[13].replace("ñ","nh"), row[15], row[18],
                         row[22], row[27], row[29], row[33],
                         row[38], row[42]))
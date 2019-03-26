import anycsv
import csv

#reader = anycsv.reader(filename="filename")
reader = anycsv.reader(url="https://dev.inpher.io/datasets/correlation/test1/bank-full-X.csv")

with open('testfile.csv', 'w') as f:
    writer = csv.writer(f, delimiter='|')
    writer.writerows([row for row in reader])

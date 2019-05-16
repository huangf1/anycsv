import anycsv
import csv

reader = anycsv.reader(filename="data.csv")
# reader = anycsv.reader(url="https://dev.inpher.io/datasets/correlation/test1/bank-full-X.csv")

with open('result.csv', 'w') as f:
    writer = csv.writer(f, delimiter='\t')
    writer.writerows([row for row in reader])
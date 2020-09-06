import os
import csv

# path = 'sse'
path = 'szse'
dirs = os.listdir(path)
fwrite = open('../totalszse.csv', 'w', encoding='ANSI', newline='')
csv_writer = csv.writer(fwrite)

for sseFile in dirs:
    print(sseFile)
    # f = open('sse/' + sseFile, 'r', encoding='ANSI')
    f = open('szse/'+sseFile, 'r', encoding='ANSI')
    reader = csv.reader(f)
    csvlist = list(reader)
    for row in range(1, len(csvlist)):
        csvlist[row][1] = int(csvlist[row][1][1:])
        for i in range(len(csvlist[row])):
            if csvlist[row][i] == 'None':
                csvlist[row][i] = ''
        # print(csvlist[row])
        csv_writer.writerow(csvlist[row])
    f.close()
fwrite.close()
import csv


def getstuff(filename):
    with open(filename, "r") as csvfile:
        datareader = csv.reader(csvfile)
        yield next(datareader)  # yield the header row
        for row in datareader:
                yield row





for i in getstuff("url_cam.csv"):
    print(i[0])




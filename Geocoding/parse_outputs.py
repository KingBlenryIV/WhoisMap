import mysql.connector
import csv
import os


db = mysql.connector.connect(
  host="localhost",
  user="blen",
  password="blen",
  database="arin"
)
cursor = db.cursor()

data = []
for filename in os.listdir("output"):
    print(filename)
    with open("output/"+filename, mode='r', newline='') as file:
        csv_reader = csv.reader(file)
        for row in csv_reader:
            id = row[0]
            match = row[2]
            exact = ""
            coordinates = ""
            tigerline = ""
            if len(row) > 3:
                exact = row[3]
                long_lat = row[5].split(",")
                coordinates = long_lat[1]+","+long_lat[0] # Census provides long,lat - change to lat,long
                tigerline = row[6] + "_" + row[7]
            data.append([match,exact,coordinates,tigerline,id])
    cursor.executemany("UPDATE locations SET is_match = %s, is_exact = %s, coordinates = %s, tigerline = %s WHERE id = %s", data)
    db.commit()
    data = []
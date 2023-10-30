import mysql.connector 
import csv


db = mysql.connector.connect(
  host="localhost",
  user="blen",
  password="blen",
  database="arin"
)
cursor = db.cursor()

cursor.execute("SELECT id, streetAddress, city, state, postalCode FROM locations")

columns = cursor.description 
locations = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]

data = []
input_file = "input_file"
i = 1
for row in locations:
    data.append([row['id'],row['streetAddress'].replace(",",""), row['city'], row['state'], row['postalCode'].split("-")[0]])
    if i % 10000 == 0:
        input_file += (str(int(i / 10000)) + ".csv")
        with open(input_file, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerows(data)
        data = []
    i += 1
    input_file = "input_file"

if len(data) > 0:
    input_file += (str(int(i / 10000) + 1) + ".csv")
    with open(input_file, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerows(data)
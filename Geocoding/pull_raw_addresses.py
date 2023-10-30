import mysql.connector 


db = mysql.connector.connect(
  host="localhost",
  user="blen",
  password="blen",
  database="arin"
)
cursor = db.cursor()

create_locations_table = '''
CREATE TABLE locations (
    id              INT PRIMARY KEY AUTO_INCREMENT,
    orgHandle       VARCHAR(255),
    streetAddress   VARCHAR(255),
    city            VARCHAR(255),
    state           VARCHAR(255),
    postalCode      VARCHAR(255),
    is_match        VARCHAR(255),
    is_exact        VARCHAR(255),
    coordinates     VARCHAR(255),
    tigerline       VARCHAR(255)
);
'''

try:
    cursor.execute(create_locations_table)
except:
    cursor.execute("DROP TABLE locations")
    cursor.execute(create_locations_table)


cursor.execute("SELECT handle, streetAddress, city, state, postalCode FROM org")

columns = cursor.description 
unfilteredLocations = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]

locations = []
i = 0
for row in unfilteredLocations:
    if row['streetAddress'] != "Private Address":
        locations.append([row['handle'],row['streetAddress'],row['city'],row['state'],row['postalCode']])
        if len(locations) > 2000:
            cursor.executemany("INSERT INTO locations (orgHandle, streetAddress, city, state, postalCode) VALUES (%s,%s,%s,%s,%s)", locations)
            db.commit()
            locations = []
    if i % 1000 == 0:
        print(i)
    i+=1

if len(locations) > 0:
    cursor.executemany("INSERT INTO locations (orgHandle, streetAddress, city, state, postalCode) VALUES (%s,%s,%s,%s,%s)", locations)
    db.commit()

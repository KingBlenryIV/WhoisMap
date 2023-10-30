#### No ISPs or CSPs
import mysql.connector
import json

def clean_ip(ip, version):
    if version < 5:
        return ".".join([str(int(i)) for i in ip.split(".")]) 
    else:
        parts = ip.split(':')
        return ':'.join([part.lstrip('0') for part in parts])

db = mysql.connector.connect(
  host="localhost",
  user="blen",
  password="blen",
  database="arin"
)

cursor = db.cursor()

cursor.execute('''
SELECT 
net.version,
org.name,
netblock.startAddress,
netblock.endAddress,
netblock.cidrLength,
locations.coordinates
from net
INNER JOIN netblock ON net.handle = netblock.netHandle
INNER JOIN locations ON net.orgHandle = locations.orgHandle
INNER JOIN org ON net.orgHandle = org.handle
where net.orgHandle
not in 
( -- Not an ISP/CSP
	SELECT 
    parentOrgHandle
    from org
)
and org.parentOrgHandle = "" -- Not an ISP/CSP customer organization
and locations.coordinates != ""
and org.handle != "IANA"
and coordinates 
''')

data = []
i = 0
for row in cursor.fetchall():
    start = clean_ip(row[2], row[0])
    end = clean_ip(row[3], row[0])

    dict = {
        "v": row[0],
        "org": row[1],
        "start": start,
        "end": end,
        "cidr": row[4],
        "coords": row[5]
    }
    data.append(dict)
    i += 1
    print(i)

mapping_json = json.dumps(data, indent = 4)
with open("map.json", "w") as outfile:
    outfile.write(mapping_json)

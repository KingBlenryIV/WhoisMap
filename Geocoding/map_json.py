import mysql.connector
import json

db = mysql.connector.connect(
  host="localhost",
  user="blen",
  password="blen",
  database="arin"
)

cursor = db.cursor()

# get all ASN nets
cursor.execute('''
SELECT handle, orgHandle
FROM net
WHERE parentNetHandle
IN (
  SELECT handle
  FROM net
  WHERE parentNetHandle = ""
)
AND orgHandle
in (
  SELECT orgHandle
  FROM asn
)
''')

asnOrgNets = {} #orgHandle: [netHandles]
for row in cursor.fetchall(): #(handle, orgHandle)
    if row[1] not in asnOrgNets.keys():
        asnOrgNets[row[1]] = [row[0]]
    else:
        asnOrgNets[row[1]].append(row[0])

cursor.execute("SELECT handle, orgHandle, parentNetHandle, version, cidrLength, coordinates FROM mapping WHERE coordinates != \"\"")

mapping_data4 = {}
mapping_data6 = {}

i = 0
for row in cursor.fetchall():
    for asnOrg, nets in asnOrgNets.items():
        if row[2] in nets: # check if the parentNetHandle belongs to the asnOrg
            coords = row[5].split(",")
            coords[0] = coords[0][:-9]
            coords[1] = coords[1][:-9]
            coords = ",".join(coords)
            data = {
                "net": row[0],
                # "org": row[1],
                # "parent": row[2],
                "cidr": row[4],
                "coords": coords
            }
            if row[3] == 4:
                if asnOrg in mapping_data4.keys():
                    mapping_data4[asnOrg].append(data)
                else:
                    mapping_data4[asnOrg] = [data]
            if row[3] == 6:
                if asnOrg in mapping_data6.keys():
                    mapping_data6[asnOrg].append(data)
                else:
                    mapping_data6[asnOrg] = [data]
            i += 1
            if i % 1000 == 0:
                print(i)
            break



mapping_json4 = json.dumps(mapping_data4, indent = 4)
mapping_json6 = json.dumps(mapping_data6, indent = 4)

with open("ipv4.json", "w") as outfile:
    outfile.write(mapping_json4)

with open("ipv6.json", "w") as outfile:
    outfile.write(mapping_json6)
 
import mysql.connector 
import xml.etree.ElementTree as ET

asns = []
orgs = []
nets = []
netblocks = []

def add_asn(data):
    element = ET.fromstring(data)
    orgHandleElement = element.find("orgHandle")
    nameElement = element.find("name")
    registrationDateElement = element.find("registrationDate")
    updateDateElement = element.find("updateDate")

    orgHandle = ""
    if orgHandleElement is not None:
        orgHandle = orgHandleElement.text
    
    name = ""
    if nameElement is not None:
        name = nameElement.text
    
    registrationDate = ""
    if registrationDateElement is not None:
        registrationDate = registrationDateElement.text

    updateDate = ""
    if updateDateElement is not None:
        updateDate = updateDateElement.text

    # insert into asn table
    global asns
    asns.append((orgHandle, name, registrationDate, updateDate))
    if len(asns) == 3000:
        cursor.executemany("INSERT INTO asn (orgHandle, name, registrationDate, updateDate) VALUES (%s,%s,%s,%s)", asns)
        db.commit()
        asns = []

def add_org(data):
    element = ET.fromstring(data)
    handleElement = element.find("handle")
    parentOrgHandleElement = element.find("parentOrgHandle")
    nameElement = element.find("name")
    countryElement = element.find("iso3166-1").find("name")
    stateElement = element.find("iso3166-2")
    cityElement = element.find("city")
    postalCodeElement = element.find("postalCode")
    streetAddressElement = element.find("streetAddress")
    registrationDateElement = element.find("registrationDate")
    updateDateElement = element.find("updateDate")

    handle = ""
    if handleElement is not None:
        handle = handleElement.text

    parentOrgHandle = ""
    if parentOrgHandleElement is not None:
        parentOrgHandle = parentOrgHandleElement.text

    name = ""
    if nameElement is not None:
        name = nameElement.text

    country = ""
    if countryElement is not None:
        country = countryElement.text

    state = ""
    if stateElement is not None:
        state = stateElement.text

    city = ""
    if cityElement is not None:
        city = cityElement.text

    postalCode = ""
    if postalCodeElement is not None:
        postalCode = postalCodeElement.text

    streetAddress = ""
    if streetAddressElement is not None:
        for line in streetAddressElement:
            if line.text is not None:
                streetAddress += line.text + " "
        streetAddress = streetAddress[:-1]

    registrationDate = ""
    if registrationDateElement is not None:
        registrationDate = registrationDateElement.text

    updateDate = ""
    if updateDateElement is not None:
        updateDate = updateDateElement.text

    # insert into org table
    global orgs
    orgs.append((handle, parentOrgHandle, name, country, state, city, postalCode, streetAddress, registrationDate, updateDate))
    if len(orgs) == 3000:
        cursor.executemany("INSERT INTO org (handle, parentOrgHandle, name, country, state, city, postalCode, streetAddress, registrationDate, updateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", orgs)
        db.commit()
        orgs = []
    

def add_net(data):
    element = ET.fromstring(data) 
    nameElement = element.find("name")
    orgHandleElement = element.find("orgHandle")
    versionElement = element.find("version")
    handleElement = element.find("handle")
    parentNetHandleElement = element.find("parentNetHandle")
    registrationDateElement = element.find("registrationDate")
    updateDateElement = element.find("updateDate")

    name = ""
    if nameElement is not None:
        name = nameElement.text

    orgHandle = ""
    if orgHandleElement is not None:
        orgHandle = orgHandleElement.text

    version = ""
    if versionElement is not None:
        version = versionElement.text

    handle = ""
    if handleElement is not None:
        handle = handleElement.text

    parentNetHandle = ""
    if parentNetHandleElement is not None:
        parentNetHandle = parentNetHandleElement.text

    registrationDate = ""
    if registrationDateElement is not None:
        registrationDate = registrationDateElement.text                                                    

    updateDate = ""
    if updateDateElement is not None:
        updateDate = updateDateElement.text

    # insert into net table
    global nets
    nets.append((name, orgHandle, version, handle, parentNetHandle, registrationDate, updateDate))
    if len(nets) == 3000:
        cursor.executemany("INSERT INTO net (name, orgHandle, version, handle, parentNetHandle, registrationDate, updateDate) VALUES (%s,%s,%s,%s,%s,%s,%s)", nets)
        db.commit()
        nets = []

    netBlocksElement = element.find("netBlocks")
    for netBlockElement in netBlocksElement.findall("netBlock"):
        startAddressElement = netBlockElement.find("startAddress")
        endAddressElement = netBlockElement.find("endAddress")
        cidrLengthElement = netBlockElement.find("cidrLenth") # ARIN mispelled length
        typeElement = netBlockElement.find("type")

        startAddress = ""
        if startAddressElement is not None:
            startAddress = startAddressElement.text

        endAddress = ""
        if endAddressElement is not None:
            endAddress = endAddressElement.text

        cidrLength = ""
        if cidrLengthElement is not None:
            cidrLength = cidrLengthElement.text

        net_type = ""
        if typeElement is not None:
            net_type = typeElement.text

        global netblocks
        netblocks.append((handle, startAddress, endAddress, cidrLength, net_type))
        if len(netblocks) == 3000:
            cursor.executemany("INSERT INTO netblock (netHandle, startAddress, endAddress, cidrLength, type) VALUES (%s,%s,%s,%s,%s)", netblocks)
            db.commit()
            netblocks = []


### MySQL
db = mysql.connector.connect(
  host="localhost",
  user="blen",
  password="blen"
)

cursor = db.cursor()
try:
    cursor.execute("CREATE DATABASE arin")
except:
    cursor.execute("DROP DATABASE arin")
    cursor.execute("CREATE DATABASE arin")

db = mysql.connector.connect(
  host="localhost",
  user="blen",
  password="blen",
  database="arin"
)
cursor = db.cursor()

create_asn_table = '''
CREATE TABLE asn (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    orgHandle        VARCHAR(255),
    name             VARCHAR(255),
    registrationDate VARCHAR(255),
    updateDate       VARCHAR(255)
);
'''

create_org_table = '''
CREATE TABLE org (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    handle           VARCHAR(255),
    parentOrgHandle  VARCHAR(255),
    name             VARCHAR(255),
    country          VARCHAR(255),
    state            VARCHAR(255),
    city             VARCHAR(255),
    postalCode       VARCHAR(255),
    streetAddress    VARCHAR(255),
    registrationDate VARCHAR(255),
    updateDate       VARCHAR(255)
);
'''

create_net_table = '''
CREATE TABLE net (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    name             VARCHAR(255),
    orgHandle        VARCHAR(255),
    version          VARCHAR(255),
    handle           VARCHAR(255),
    parentNetHandle  VARCHAR(255),
    registrationDate VARCHAR(255),
    updateDate       VARCHAR(255)
);
'''

create_netblock_table = '''
CREATE TABLE netblock (
    id               INT PRIMARY KEY AUTO_INCREMENT,
    netHandle        VARCHAR(255),
    startAddress     VARCHAR(255),
    endAddress       VARCHAR(255),
    cidrLength       VARCHAR(255),
    type             VARCHAR(255)
);
'''

cursor.execute("BEGIN")
cursor.execute(create_asn_table)
cursor.execute(create_org_table)
cursor.execute(create_net_table)
cursor.execute(create_netblock_table)
db.commit()

with open("arin_db.xml", "r", errors="ignore") as file:
    i = 0
    element_text = ""
    appending = False
    in_pocs = False
    for line in file:
        line_text = line.replace('\n', '').replace(' ', '')
        if line_text == "<poc>":
            in_pocs = True
        if line_text == "<org>":
            in_pocs = False
        if in_pocs:
            i+=1
            continue
        match line_text:
            case "<asn>":
                element_text += line
                appending = True
            case "<org>":
                element_text += line
                appending = True
            case "<net>":
                element_text += line
                appending = True
            case "</asn>":
                element_text += line
                add_asn(element_text)
                element_text = ""
                appending = False
            case "</org>":
                element_text += line
                add_org(element_text)
                element_text = ""
                appending = False
            case "</net>":
                element_text += line
                add_net(element_text)
                element_text = ""
                appending = False
            case _:
                if appending:
                    element_text += line
        i+=1
        if i % 1000 == 0:
            print(i)

if len(asns) > 0:
    cursor.executemany("INSERT INTO asn (orgHandle, name, registrationDate, updateDate) VALUES (%s,%s,%s,%s)", asns)
    db.commit()
if len(orgs) > 0:
    cursor.executemany("INSERT INTO org (handle, parentOrgHandle, name, country, state, city, postalCode, streetAddress, registrationDate, updateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", orgs)
    db.commit()
if len(nets) > 0:
    cursor.executemany("INSERT INTO net (name, orgHandle, version, handle, parentNetHandle, registrationDate, updateDate) VALUES (%s,%s,%s,%s,%s,%s,%s)", nets)
    db.commit()
if len(netblocks) > 0:
    cursor.executemany("INSERT INTO netblock (netHandle, startAddress, endAddress, cidrLength, type) VALUES (%s,%s,%s,%s,%s)", netblocks)
    db.commit()

db.close()
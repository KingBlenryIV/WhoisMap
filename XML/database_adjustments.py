import mysql.connector
import xml.etree.ElementTree as ET


db = mysql.connector.connect(
  host="localhost",
  user="blen",
  password="blen",
  database="arin"
)

cursor = db.cursor()

# Add numbers and handles to asn

asns=[]
def add_asn(data):
    element = ET.fromstring(data)
    orgHandleElement = element.find("orgHandle")
    numberElement = element.find("startAsNumber")
    handleElement = element.find("handle")
    nameElement = element.find("name")
    registrationDateElement = element.find("registrationDate")
    updateDateElement = element.find("updateDate")
    
    handle = ""
    if handleElement is not None:
        handle = handleElement.text

    number = ""
    if numberElement is not None:
        number = numberElement.text

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
    asns.append((handle, number, orgHandle, name, registrationDate, updateDate))
    if len(asns) == 3000:
        cursor.executemany("INSERT INTO asn (handle, number, orgHandle, name, registrationDate, updateDate) VALUES (%s,%s,%s,%s,%s,%s)", asns)
        db.commit()
        asns = []

with open("arin_db.xml", "r", errors="ignore") as file:
    i = 0
    element_text = ""
    appending = False
    for line in file:
        line_text = line.replace('\n', '').replace(' ', '')
        if line_text == "<poc>":
            break
        match line_text:
            case "<asn>":
                element_text += line
                appending = True
            case "</asn>":
                element_text += line
                add_asn(element_text)
                element_text = ""
                appending = False
            case _:
                if appending:
                    element_text += line
        i+=1
        if i % 1000 == 0:
            print(i)

if len(asns) > 0:
    cursor.executemany("INSERT INTO asn (handle, number, orgHandle, name, registrationDate, updateDate) VALUES (%s,%s,%s,%s,%s,%s)", asns)
    db.commit()

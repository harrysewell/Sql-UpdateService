import pyodbc
import requests
import logging

# Define My Globals:
connection = pyodbc.connect('DRIVER={SQL Server};SERVER=MTGSRV067;DATABASE=!!!TEST!!!;UID =MTG_LONDON\harrsewe;PWD =Monday123')
print("Connection Successfully Established")
cursor = connection.cursor()
mylogfile ='.\log.log'


# aet up logging
logger = logging.getLogger('myapp')
hdlr = logging.FileHandler('.\log.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)


SQLCommandPRG = """(select szprghouse 'Product Code', ttl1 'Title'
from prg
where szprghouse in
(
       select szprghouse 'House No'
       --, CONVERT (varchar,[dbo].[BSSDateTO_SQLDate](changedate),103) 'Change Date'
       --,CONVERT(varchar,dbo.MinuteTimeToString_HH_MM_SS(prg.ChangeTime),108)'Change Time'
       --, CONVERT (varchar, GETDATE(),108)  'Current Time'
       --,CONVERT(varchar,dateadd(hh,-1,GETDATE()),108) '1 hr ago'
       from prg
       where
       dbo.SQLDateTO_BSSDate(GetDate()) = prg.changedate
       and CONVERT(varchar,dbo.MinuteTimeToString_HH_MM_SS(prg.ChangeTime),108) >=CONVERT(varchar,dateadd(hh,-1,GETDATE()),108)
))"""i

# execute my programme query
cursor.execute(SQLCommandPRG)
# retValue = cursor.fetchall()

for row in cursor.fetchall():
    title = row[1]
    houseNumber =row[0]
    print(title, houseNumber)
    EidrUpdate = """<?xml version='1.0' encoding='utf-8'?>
    <product>
        <EIDR>"""+title+"""</EIDR>
    </product>"""
    headers = {'Content-Type': 'application/xml'}  # set what the server accepts
    # print
    print(houseNumber)
    print(requests.post('http://mtgsrv054:6626/EIDR/updateEIDR/'+houseNumber, data=EidrUpdate, headers=headers).text)
    logger.info('POSTING an update to ProductCode "'+houseNumber+'" with the EIDR ID "'+title+'"')
# closing connection
connection.close()

#response = requests.get("http://api.open-notify.orgi/iss-now.json")

# Print the status code of the response.
#print(response.status_code)
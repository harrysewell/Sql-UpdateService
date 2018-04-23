import pyodbc
import requests
import smtplib
import logging
import sys
import xml.etree.ElementTree as etree
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ====================================================================================================================#
# BSS to WON Update Interface Script                                                                                  #
# ====================================================================================================================#
# VERSION:       V.10                                                                                                 #
# AUTHOR:        Harry.Sewell@mtg.com                                                                                 #
#                                                                                                                     #
# SCRIPT FUNCTION: The Script servers as a proof of concept to allow us to take data from BSS in the form of a PSQL   #
#                  cursor return and query/compare data gleaned from one or more WON API methods.                     #
#                  Presently for each updated PRG record in BSS the WON Viaplay SOAP api is called to compare BSS     #
#                  "title" and WON "BSS/LMK title". If the titles do not match or do not exist then I am passing the  #
#                  BSS title to a POST  request to the WON EIDR API.                                                  #
#                                                                                                                     #
# REQUIREMENTS:    python 3.6 and modules defined above, network access to API endpoints, Smtp server and DB.         #
#                                                                                                                     #
#                                                                                                                     #
# ====================================================================================================================#
# Define Globals                                                                                                      #
# ====================================================================================================================#

mylogfile = '.\log.log'
sys.tracebacklimit = 1

# sql variables:
SQLServer = 'MTGSRV067'
SQLdb = '!!!TEST!!!'
SQLUsername = 'MTG_LONDON\claielli'
SQLPWD = 'Monday123'

# API endpoint variables:
WonEIDRendPoint = 'http://mtgsrv054:6626/EIDR/updateEIDR/'
WOnViaplaySOAPurl = 'http://mtgsrv054:6609/ViaplayWhatsONSoapService'

# Email variables:
SmtpServer = '10.253.144.22'
FromAddress = 'Bss2WonUpdateService@mtg.com'
Toaddress = 'harry.sewell@mtg.com'
smtpObj = smtplib.SMTP(SmtpServer)
msg = MIMEMultipart()
msg['From'] = FromAddress
msg['To'] = Toaddress
msg['Subject'] = "Bss2Won Service Alert"

# ====================================================================================================================#
# Define SQL Queries                                                                                                  #
# ====================================================================================================================#

# Programme Level Query
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
))"""

# Series level query

SQLCommandSRS = """(select ulcode 'Product Code', ttl1 'Title'
from srs
where ulcode in
(
       select ulcode 'House No'
       --, CONVERT (varchar,[dbo].[BSSDateTO_SQLDate](changedate),103) 'Change Date'
       --,CONVERT(varchar,dbo.MinuteTimeToString_HH_MM_SS(prg.ChangeTime),108)'Change Time'
       --, CONVERT (varchar, GETDATE(),108)  'Current Time'
       --,CONVERT(varchar,dateadd(hh,-1,GETDATE()),108) '1 hr ago'
       from srs
       where
       dbo.SQLDateTO_BSSDate(GetDate()) = srs.changedate
       and CONVERT(varchar,dbo.MinuteTimeToString_HH_MM_SS(srs.ChangeTime),108) >=CONVERT(varchar,dateadd(hh,-1,GETDATE()),108)
))"""

# ====================================================================================================================#
# Set up logging parameters                                                                                           #
# ====================================================================================================================#

logger = logging.getLogger('myapp')
hdlr = logging.FileHandler('.\log.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)

# ====================================================================================================================
# Connect to DB
# ====================================================================================================================
try:
    connection = pyodbc.connect('DRIVER={SQL Server};SERVER='+SQLServer+';DATABASE='+SQLdb+';UID ='+SQLUsername+';PWD ='+SQLPWD)
    print('Successfully Connected to database:'+SQLdb)
    logger.info('Successfully Connected to database:'+SQLdb)
    cursor = connection.cursor()
except Exception as Error:
    print('we had an error: cannot connect to '+SQLServer+':'+SQLdb)
    msg['Subject'] = msg['Subject']+' - DB Connection Error!'
    msg.attach(MIMEText(f"""
We've had an exception: Cannot connect to database:{SQLdb} on server: {SQLServer} wih user: {SQLUsername}
    
The details of the exception are: 

{Error}
    
    """, 'plain'))

    # send an email of complaint:
    smtpObj.sendmail(FromAddress, Toaddress, msg.as_string())
    logger.exception('Cannot connect to database: '+SQLdb+'on server: '+SQLServer+' wih user: '+SQLUsername)
    # quit application
    raise SystemExit


# ====================================================================================================================#
# SQL Function                                                                                                        #
# ====================================================================================================================#

def RunSQL (Query):

    cursor.execute(Query)
    return cursor.fetchall()


def EidrPost (ProductCode, EidrID):

    # Format XML body here:
    EidrUpdate = """<?xml version='1.0' encoding='utf-8'?>
    <product>
        <EIDR>"""+EidrID+"""</EIDR>
    </product>"""
    headers = {'Content-Type': 'application/xml'}  # set what the server accepts

    try:
        logger.info('POSTING an update to ProductCode "'+ProductCode+'" with the EIDR ID "'+EidrID+'"')
        requests.post(WonEIDRendPoint+houseNumber, data=EidrUpdate, headers=headers)
        logger.info('Housenumber '+ProductCode+' Updated successfully')
    except requests.exceptions.RequestException as Error:
        logger.error('Housenumber: '+ProductCode+' Failed to update with EIDR code:"'+EidrID+'"')
        logger.exception(f'details of the Exception:', exc_info=1)
        raise SystemExit

# ====================================================================================================================#
# WON SOAP FUNCTION                                                                                                   #
# ====================================================================================================================#
def SoapWONReq(ProductCode, url):

    SoapHeader = {'content-type': 'text/xml'}
    SoapRequestBody = f"""
    
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:api="http://api.whatson.viasat.tv/">
    <soapenv:Header>
      <api:BasicAuthHeader soapenv:mustUnderstand="1">
         <Name>psi</Name>
         <Password>whatson</Password>
      </api:BasicAuthHeader>
   </soapenv:Header>
   <soapenv:Body>
      <api:getProductByProductCode>
            <!--1 or more repetitions:-->
            <productCode>{ProductCode}</productCode>
        </api:getProductByProductCode>
        </soapenv:Body>
    </soapenv:Envelope>"""
    response = requests.post(url, data=SoapRequestBody, headers=SoapHeader)
    xml = etree.fromstring(response.text)

    # define my response as variable
    soapresponse = etree.fromstring(response.content)

    # create variables using xpaths

    #Won may not have the required field, if attribute error catch this in an exception.
    try:
        WON_BSSLmnktitle = soapresponse.find(".//p_product_producttitles/ES_ProductTitle/pt_type/ESP_ProductTitleType[@name='BSS/Landmark Title']....").attrib
        WON_subcategory = soapresponse.find(".//ES_Product/p_product_subcategory/ESP_SubCategory").attrib
        # Select values from dictionary objects:
        WON_BSSLmnktitle = WON_BSSLmnktitle.get('pt_title', 'Not found')
        WON_subcategory = WON_subcategory.get('name', 'Not found')
    except AttributeError as Error:
        print('No value found at the required xpath')
        WON_BSSLmnktitle = 'Not found'
        WON_subcategory = 'Not found'


    return WON_BSSLmnktitle, WON_subcategory


# ====================================================================================================================#
# MAIN PROGRAMME                                                                                                      #
# ====================================================================================================================#

# Query BSS for a list of programmes that have been updated in the last hour:
BssChanged = RunSQL(SQLCommandPRG)

# Iterate through each record and query the WON Api comparing bss/LMK title:
if BssChanged != []:
    print(f'these records have been updated:{BssChanged}')
    for row in BssChanged:
        # set title and houseno to human readable variables:
        title = row[1]
        houseNumber = row[0]

        # now for each changed item query the WON API to check if fields match:
        SoapResult = SoapWONReq(houseNumber, WOnViaplaySOAPurl)

        if SoapResult[0] == 'Not found':
            # means the required xpath is not present so we update WON.
            print('no value held in WON so we must update')
            EidrPost(houseNumber, title)

        else:
            # we need to compare the BSS title with the one held in WON:
            print(title)
            print(SoapResult[0])
            if title == SoapResult[0]:
                # The titles match in BSS and WON so no need to update:
                print('the title matches so no update is needed')
                logger.info(f'No Update needed: "{houseNumber}" has the BSS title "{title}" while WON has the BSS/Landmark title {SoapResult[0]}')
            else:
                # Titles are different so we need to update WON:
                print('titles are different, so i need to update WON!')
                EidrPost(houseNumber, title)

else:
    # the query result was NULLL so therefore we do nothing.
    print('The Query returned null')
    logger.info('Query returned Null - No records changed in the last hour')

connection.close()


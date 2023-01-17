import os
import sys
import petl
import pymssql
import configparser
import requests
import datetime
import json
import decimal


config = configparser.ConfigParser()
try:    
    config.read('ETLDemo.ini')
except Exception as e:
    print("Coulndt read configuration file: " + str(e) )
    sys.exit()

startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print("Couldnt make request: " + str(e))
    sys.exit()

BOCDates = []
BOCRates = []

#check response status
if(BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)
    #extract observation data into column arrays
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'], '%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXUSDCAD']['v']))

    #create petl table from column arrays and rename columns
    exchangeRates = petl.fromcolumns([BOCDates,BOCRates],header=['date','rate'])

    #load expense document
    try:
        expenses = petl.io.xlsx.fromxlsx("Expenses.xlsx",sheet="Github")
    except Exception as e:
        print("coulndt open expenses.xlsx: " + str(e))
        sys.exit()

    #join tables
    expenses = petl.outerjoin(exchangeRates,expenses,key="date")
    #fill down misisng values
    expenses = petl.filldown(expenses,"rate")
    #remove dates with no expenses
    expenses = petl.select(expenses, lambda rec: rec.USD != None)
    #add CDN column
    expenses = petl.addfield(expenses,"CAD", lambda rec: decimal.Decimal(rec.USD) * rec.rate)

    #initialize database connection
    try:
        dbConnection = pymssql.connect(server=destServer,database=destDatabase)
    except Exception as e:
        print("couldnt connect the database: " + str(e))

    #populates Expenses database table
    try:
        petl.io.todb(expenses,dbConnection,"Expenses")
    except Exception as e:
        print("couldnt write database " + str(e))
    
    print(expenses)
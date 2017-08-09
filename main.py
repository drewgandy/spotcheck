
import time
import config as cfg
startclock = time.clock()
#import MySQLdb as mdb
#import mysqlclient as mdb
#import _mysql
import sys

#import CA_Denoise_Name
import CA_Import_UCC_Data
#import Compare_Data
#import CA_Import_Corp_Data
#import symspell
#import symspell_db

print("START: " + time.ctime())
CA_Import_UCC_Data.Import_UCC_Data()
#CA_Import_Corp_Data.Import_Corp_Data()


#print("COMPARE START")
#Compare_Data.CompareData_OnMySQLServer()

#Compare_Data.CompareData_InPython()
#symspell_db.checkspell_DB()

print("DONE: " + time.ctime())
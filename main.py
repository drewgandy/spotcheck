import time
#import CA_Denoise_Name
import CA_Import
#import Compare_Data
#import CA_Import_Corp_Data
#import symspell
#import symspell_db

print("START: " + time.ctime())
#CA_Import.Import_UCC_Data()
CA_Import.generate_infile_DAT_files()
CA_Import.Import_UCC_Data_infile()
#CA_Import.strip_whitespace()
#CA_Import.Import_Corp_Data()
#print("COMPARE START")
#Compare_Data.CompareData_OnMySQLServer()

#Compare_Data.CompareData_InPython()
#symspell_db.checkspell_DB()

print("DONE: " + time.ctime())

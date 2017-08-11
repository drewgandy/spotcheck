import re
from unidecode import unidecode
import MySQLdb as mdb
import logging

def generate_infile_DAT_files(record=''):
    import glob
    import config as cfg
    
    logging.basicConfig(filename='UCC_import.log',format='%(asctime)s %(message)s', level=logging.DEBUG)

    path = cfg.UCC_path

    pct = 1
    total_linecount = 22805193
    if record == 1 or record == '': 
        f_initial_filing_record = open(path+"ifr.dat","w")
    if record == 2 or record == '': 
        f_business_debtor = open(path+"bd.dat","w")
    if record == 3 or record == '': 
        f_personal_debtor = open(path+"pd.dat","w")
    if record == 4 or record == '': 
        f_business_secured_party = open(path+"bsp.dat","w")
    if record == 5 or record == '': 
        f_personal_secured_party = open(path+"psp.dat","w")
    if record == 7 or record == '': 
        f_collateral = open(path+"c.dat","w")
    if record == 9 or record == '': 
        f_checksum = open(path+"checksum.dat","w")

    linecount = 0
    for filename in glob.iglob(path + '*.txt'):

        logging.info("READING "+filename)

        f = open(filename, 'r', encoding='latin-1', errors='surrogateescape')
        for t in f:
            linecount = linecount + 1

            # this can limit the number of records looped through
            #if linecount == 10:
                #con.commit()
                #con.close()
            #    logging.debug("BREAK EARLY##")
                #break
            
            RecordCode = t[0:1]
            #this IF statement checks to make sure line doesn't have junk info
            #by seeing if first 15 characters of line are a number and
            #if length of those 15 characters, after whitespace is stripped, is
            #greater than 8 (which is the minimum length of old UCC filings)
            if t[0:15].strip().isdigit() is True and len(t[0:15].strip()) > 8:
                #initial filing record
                if RecordCode == "1" and len(t.rstrip('\n')) <= 650 and (record == '' or record == int(RecordCode)):
                    f_initial_filing_record.write(''.join([t[1:15], t[27:57], "\n"]))
                #business debtor
                if RecordCode == "2" and len(t.rstrip('\n')) <= 650 and (record == '' or record == int(RecordCode)):
                    f_business_debtor.write(''.join([t[1:15], t[27:327], DeNoiseName(t[27:327].strip()).ljust(300), t[327:557], "\n"]))
                #personal debtor    
                if RecordCode == "3" and len(t.rstrip('\n')) <= 649 and (record == '' or record == int(RecordCode)):
                    f_personal_debtor.write(''.join([t[1:15], t[27:413], "\n"]))
                #business secured party
                if RecordCode == "4" and len(t.rstrip('\n')) <= 650 and (record == '' or record == int(RecordCode)):
                    f_business_secured_party.write(''.join([t[1:15], t[27:557], "\n"]))
                #personal secured party
                if RecordCode == "5" and len(t.rstrip('\n')) <= 649 and (record == '' or record == int(RecordCode)):
                    f_personal_secured_party.write(''.join([t[1:15], t[27:413], "\n"]))
                #change Filing (UCC3)
                #if RecordCode == "6":
                    #print "Initial Filing Number: "+t[2:15]
                #collateral
                if RecordCode == "7" and len(t.rstrip('\n')) <= 647 and (record == '' or record == int(RecordCode)):
                    f_collateral.write(''.join([t[1:15], t[27:123], "\n"]))
            #checksum
            if RecordCode == "9" and len(t.rstrip('\n')) <= 516 and (record == '' or record == int(RecordCode)):
                f_checksum.write(''.join([t[27:109], "\n"]))

            if ((linecount / total_linecount)*100) > pct:
                pct = pct + 1
                logging.info(str(int((linecount / total_linecount)*100)) + "%")
                #print(str(int((linecount / total_linecount)*100)) + "%")
        logging.info("CLOSING " + filename)
    f.close()

def Import_UCC_Data_infile(record=''):
    import glob
    import config as cfg
    
    logging.basicConfig(filename='UCC_import.log',format='%(asctime)s %(message)s', level=logging.DEBUG)

    con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'])

    cur = con.cursor()
    if record == 1 or record == '': 
        cur.execute("DROP TABLE IF EXISTS InitialFilingRecord")
        cur.execute("CREATE TABLE InitialFilingRecord(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), InitialFilingType VARCHAR(5), FilingDate VARCHAR(8), FilingTime VARCHAR(4), FilingStatus VARCHAR(1), LapseDate VARCHAR(8), PageCount VARCHAR(4))")
        logging.info("IMPORTING IFR INTO MYSQL.")
        cur.execute("load data local infile '/home/ubuntu/code/data/CA/UCC/ifr.dat' INTO TABLE InitialFilingRecord FIELDS TERMINATED BY '' (InitialFilingNumber, InitialFilingType, FilingDate, FilingTime, FilingStatus, LapseDate, PageCount)")
        con.commit()
    if record == 2 or record == '': 
        cur.execute("DROP TABLE IF EXISTS BusinessDebtors")
        cur.execute("CREATE TABLE BusinessDebtors(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), Name VARCHAR(300), NonNoisyName VARCHAR(300), StreetAddress VARCHAR(110), City VARCHAR(64), State VARCHAR(32), ZipCode VARCHAR(15), ZipCodeExtension VARCHAR(6), CountryCode VARCHAR(3))")
        logging.info("IMPORING BusinessDebtors INTO MYSQL.")
        cur.execute("load data local infile '/home/ubuntu/code/data/CA/UCC/bd.dat' INTO TABLE BusinessDebtors FIELDS TERMINATED BY '' (InitialFilingNumber, Name, NonNoisyName, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode)")
        con.commit()
    if record == 3 or record == '': 
        cur.execute("DROP TABLE IF EXISTS PersonalDebtors")
        cur.execute("CREATE TABLE PersonalDebtors(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), LastName VARCHAR(50), FirstName VARCHAR(50), MiddleName VARCHAR(50), Suffix VARCHAR(6), StreetAddress VARCHAR(110), City VARCHAR(64), State VARCHAR(32), ZipCode VARCHAR(15), ZipCodeExtension VARCHAR(6), CountryCode VARCHAR(3))")
        logging.info("IMPORING PersonalDebtors INTO MYSQL.")
        cur.execute("load data local infile '/home/ubuntu/code/data/CA/UCC/pd.dat' INTO TABLE PersonalDebtors FIELDS TERMINATED BY '' (InitialFilingNumber, LastName, FirstName, MiddleName, Suffix, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode)")
        con.commit()
    if record == 4 or record == '': 
        cur.execute("DROP TABLE IF EXISTS BusinessSecuredParties")
        cur.execute("CREATE TABLE BusinessSecuredParties(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), Name VARCHAR(300), StreetAddress VARCHAR(110), City VARCHAR(64), State VARCHAR(32), ZipCode VARCHAR(15), ZipCodeExtension VARCHAR(6), CountryCode VARCHAR(3))")
        logging.info("IMPORING BusinessSecuredParties INTO MYSQL.")
        cur.execute("load data local infile '/home/ubuntu/code/data/CA/UCC/bsp.dat' INTO TABLE BusinessSecuredParties FIELDS TERMINATED BY '' (InitialFilingNumber, Name, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode)")
        con.commit()
    if record == 5 or record == '': 
        cur.execute("DROP TABLE IF EXISTS PersonalSecuredParties")
        cur.execute("CREATE TABLE PersonalSecuredParties(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), LastName VARCHAR(50), FirstName VARCHAR(50), MiddleName VARCHAR(50), Suffix VARCHAR(6), StreetAddress VARCHAR(110), City VARCHAR(64), State VARCHAR(32), ZipCode VARCHAR(15), ZipCodeExtension VARCHAR(6), CountryCode VARCHAR(3))")
        logging.info("IMPORING PersonalSecuredParties INTO MYSQL.")
        cur.execute("load data local infile '/home/ubuntu/code/data/CA/UCC/psp.dat' INTO TABLE PersonalSecuredParties FIELDS TERMINATED BY '' (InitialFilingNumber, LastName, FirstName, MiddleName, Suffix, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode)")
        con.commit()
    if record == 7 or record == '': 
        cur.execute("DROP TABLE IF EXISTS Collateral")
        cur.execute("CREATE TABLE Collateral(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), AssociatedFilingNumber VARCHAR(10), LineSequenceNumber VARCHAR(6), Description VARCHAR(80))")
        logging.info("IMPORING Collateral INTO MYSQL.")
        cur.execute("load data local infile '/home/ubuntu/code/data/CA/UCC/c.dat' INTO TABLE Collateral FIELDS TERMINATED BY '' (InitialFilingNumber, AssociatedFilingNumber, LineSequenceNumber, Description)")
        con.commit()
    if record == 9 or record == '': 
        cur.execute("DROP TABLE IF EXISTS Checksum")
        cur.execute("CREATE TABLE Checksum(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingRecord VARCHAR(8), BusinessDebtors VARCHAR(8), PersonalDebtors VARCHAR(8), BusinessSecuredParties VARCHAR(8), PersonalSecuredParties VARCHAR(8), ChangeFiling VARCHAR(8), Collateral VARCHAR(8))")
        logging.info("IMPORING Checksum INTO MYSQL.")
        cur.execute("load data local infile '/home/ubuntu/code/data/CA/UCC/checksum.dat' INTO TABLE Checksum FIELDS TERMINATED BY '' (InitialFilingRecord, BusinessDebtors, PersonalDebtors, BusinessSecuredParties, PersonalSecuredParties, ChangeFiling, Collateral)")
        con.commit()


def strip_whitespace(tbls=''):
    import config as cfg

    logging.basicConfig(filename='UCC_import.log',format='%(asctime)s %(message)s', level=logging.DEBUG)

    con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'])
    cur = con.cursor()

    if tbls == '':
        tables = ['InitialFilingRecord','PersonalDebtors','Collateral','BusinessDebtors', 'PersonalSecuredParties','BusinessSecuredParties']
    else:
        tables = tbls
    #tables = ['Collateral','BusinessDebtors', 'PersonalSecuredParties','BusinessSecuredParties']
    for table in tables:
        logging.info("Trimming table " + table)
        cur.execute("SHOW columns FROM %s" % table)
        columns = cur.fetchall()
        con.commit()
        for column in columns:
            print(column[0])
            if column[0] != 'Id':
                logging.info("Trimming column " + column[0])
                #sql = """UPDATE BusinessDebtors SET %s = TRIM(%s)"""
                c=column[0]
                cur.execute("UPDATE %s SET %s = TRIM(%s)"% (table, column[0],column[0]))
    logging.info("### ALL DONE.  GOOD LUCK! ###")
 
 

def Import_UCC_Data():
 #   import _mysql
 #   import CA_Denoise_Name
    import glob
    import config as cfg
    
    logging.basicConfig(filename='UCC_import.log',format='%(asctime)s %(message)s', level=logging.DEBUG)

    con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'])
    linecount = 0

    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS InitialFilingRecord")
    cur.execute("CREATE TABLE InitialFilingRecord(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), InitialFilingType VARCHAR(5), FilingDate VARCHAR(8), FilingTime VARCHAR(4), FilingStatus VARCHAR(1), LapseDate VARCHAR(8), PageCount VARCHAR(4))")
    cur.execute("DROP TABLE IF EXISTS BusinessDebtors")
    cur.execute("CREATE TABLE BusinessDebtors(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), Name VARCHAR(300), NonNoisyName VARCHAR(300), StreetAddress VARCHAR(110), City VARCHAR(64), State VARCHAR(32), ZipCode VARCHAR(15), ZipCodeExtension VARCHAR(6), CountryCode VARCHAR(3))")
    cur.execute("DROP TABLE IF EXISTS SecuredParties")
    cur.execute("CREATE TABLE SecuredParties(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), Name VARCHAR(300), StreetAddress VARCHAR(110), City VARCHAR(64), State VARCHAR(32), ZipCode VARCHAR(15), ZipCodeExtension VARCHAR(6), CountryCode VARCHAR(3))")
 
    path = cfg.UCC_path
    commit_count=0
    commit_count_target=cfg.commit_count_factor
    pct = 1
    portion= 228051
    for filename in glob.iglob(path + '*.txt'):
        logging.info("OPENING: "+filename)
        #logging.debug(filename)
        f = open(filename, 'r', encoding='latin-1', errors='surrogateescape')
        skippedlines = 0
        initialFilings = 0
        BusinessDebtorCount = 0
        linecount = 0
        linecount = 0
        for line in f:
            #logging.debug(line)
            t = line #.readline()

            # this can limit the number of records looped through
            #linecount = linecount + 1
            #if linecount == 2:
                #con.commit()
                #con.close()
            #    logging.debug("BREAK EARLY##")
            #    break
            
            RecordCode = t[0:1]
            
            if RecordCode == "1": #Initial Filing Record
                #print "#################################################"
                #initialFilings=initialFilings+1
                #logging.debug("Filing Type: Initial Filing " + str(initialFilings))
                cur.execute("INSERT INTO InitialFilingRecord(InitialFilingNumber, InitialFilingType, FilingDate, FilingTime, FilingStatus, LapseDate, PageCount) VALUES(%s, %s, %s, %s, %s, %s, %s)", (t[1:14].strip(),t[27:32].strip(),t[32:40].strip(),t[40:44].strip(),t[44:45].strip(),t[45:53].strip(),t[53:57].strip()))
                commit_count=commit_count+1

            if RecordCode == "2": #Business Debtor
                # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
                #BusinessDebtorCount=BusinessDebtorCount+1
                #print "Business Debtor " + str(BusinessDebtorCount)
                cur.execute("INSERT INTO BusinessDebtors(InitialFilingNumber, Name, NonNoisyName, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (t[1:14].strip(),t[27:327].strip(), DeNoiseName((t[27:327]).strip()), t[327:437].strip(),t[437:501].strip(),t[501:533].strip(),t[533:548].strip(),t[548:554].strip(),t[554:557].strip()))
                commit_count=commit_count+1
    
#            if RecordCode == "3": #Personal Debtor - will skip
#                logging.debug("Personal Debtor skipped " + str(skippedlines))
            
            if RecordCode == "4": #Business Secured Party
                cur.execute("INSERT INTO SecuredParties(InitialFilingNumber, Name, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);", (t[1:14].strip(),t[27:327].strip(), t[327:437].strip(),t[437:501].strip(),t[501:533].strip(),t[533:548].strip(),t[548:554].strip(),t[554:557].strip()))
                commit_count=commit_count+1

            if RecordCode == "5":
                cur.execute("INSERT INTO SecuredParties(InitialFilingNumber, Name, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);", (t[1:14].strip(),t[77:127].strip() + " " + t[127:177].strip() + " " + t[27:77].strip() + " " + t[177:183].strip(), t[183:293].strip(),t[293:357].strip(),t[357:389].strip(),t[389:404].strip(),t[404:410].strip(),t[410:413].strip()))
                commit_count=commit_count+1

            #if RecordCode == "6": #Change Filing (UCC3)
                #print "Initial Filing Number: "+t[2:15]
                #print "UCC3 Filing Number: "+t[15:27]
                #print "Change Filing Type: "+ChangeFilingType.get(t[27:32].strip(), "EMPTY")
            
            #if RecordCode == "7": #Collateral
                #skippedlines=skippedlines+1
                #print "collateral goes here - account for multiple lines of collateral, appending one after the next " + str(skippedlines)
            if commit_count == commit_count_target:
                    con.commit()
                    commit_count_target=commit_count_target+cfg.commit_count_factor
            if linecount > (portion * pct):
                logging.info(str(int((linecount/22805193)*100)) + "%")
                if pct == 1:
                    pct = 2
                else:
                    pct = pct + 1
        logging.info("DONE with " + filename)
    f.close()


def Import_Corp_Data():
    import MySQLdb as mdb
#    import _mysql
#    import CA_Denoise_Name
    import time
    import glob
#    import os
    import config as cfg

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'])

    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS SOSCompanyList")
    cur.execute("CREATE TABLE SOSCompanyList(Id INT PRIMARY KEY AUTO_INCREMENT, CompNum VARCHAR(12), FormationDate VARCHAR(8), Status VARCHAR(1), Type VARCHAR(4), Name VARCHAR(350), NonNoisyName VARCHAR(350))")

    linecount = 0
    
    path = cfg.Corp_path
    pct = 1
    portion= 186646
    for filename in glob.iglob(path + '*.TXT'):
        logging.debug(filename)
        f = open(filename, 'r', encoding='ascii', errors='surrogateescape')
        logging.debug("OPEN: " + str(time.clock()))
        linecount = 0
        linecount = 0
        for line in f:
            t = line #.readline()
            # this can limit the number of records looped through
            #linecount = linecount + 1
            #if linecount == 5:
            #    break

            if filename.endswith('CORPMASTER.TXT'):
                # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
                cur.execute("INSERT INTO SOSCompanyList(CompNum, FormationDate, Status, Type, Name, NonNoisyName) VALUES(%s, %s, %s, %s, %s, %s);", (t[5:13].strip(), t[13:21].strip(), t[21].strip(), t[22:26].strip(), t[70:420].strip(), DeNoiseName(t[70:420])))
                linecount=linecount+1

            if filename.endswith('LPMASTER.TXT'):
                # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
                cur.execute("INSERT INTO SOSCompanyList(CompNum, FormationDate, Status, Type, Name, NonNoisyName) VALUES(%s, %s, %s, %s, %s, %s);", (t[0:12].strip(), t[12:20].strip(), t[20].strip(), t[21].strip(), t[22:242].strip(), DeNoiseName(t[22:242])))
                linecount=linecount+1
            if linecount > (portion * pct):
                    logging.debug(str(int((linecount/3732920)*100)) + "%")
                    if pct == 1:
                        pct = 5
                    else:
                        pct = pct + 5
        f.close()
        logging.debug(linecount)


    # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
def DeNoiseName(NoisyName):
    # strip spaces from name and make uppercase
    NoisyName = NoisyName.strip().upper()
    
    NoisyName = unidecode(NoisyName)

    # remove punctuation and replace with space, but then strip spaces if at end of line (spaces at end of line were causing, e.g. "Inc." not to be found at end of line because period replaced with space
    for ch in ['\"', "\'", '[', ']', '{', '}', '(', ')', ':', ',', '!', '<', '>', '?', ';', '\\', '//', '.','-','`','~']:
        if ch in NoisyName:
            NoisyName=NoisyName.replace(ch," ").strip()
    
    # Replace "&" with "AND" (NOTE: CONFIRMED THAT CA SOS DOES THIS IN SEARCH LOGIC)
    NoisyName=NoisyName.replace('&', 'AND')
    
    # ignore "THE" at beginning of name
    if NoisyName.startswith('THE '):
        NoisyName = NoisyName[4:]
    
    # combine single letters together (e.g. "L L C" becomes "LLC") NOTE: I'M NOT POSITIVE CA SOS DOES THIS
    NoisyName = re.sub(r"\b(\w) (?=\w\b)", r"\1", NoisyName)
            
    # remove words and abbreviations at the end of a name that indicate the existence or nature of an organization.  This loops until all relevant words are removed. NOTE: I'M NOT POSITIVE CA SOS REMOVES 'A CALIFORNIA...'
    # NOTE: confirmed that CA SOS does also remove California from 'California LLC' etc.
    restart = True
    while restart == True:
        restart = False #reset restart flag
        for ch in ['A', 'A CALIFORNIA', 'AKA', 'AN', 'AND', 'ASSN', 'ASSOC', 'ASSOCIATES', 'ASSOCIATION', 'ASSOCS', 'AT', 'CO', 'COMPANIES', 'COMPANY', 'COMPANYS', 'COOP', 'COOPERATIVE', 'CORP', 'CORPORATION', 'DBA', 'DIV', 'DIVISION', 'FDBA', 'FKA', 'FOR','IN', 'INC', 'INCORPORATED', 'IS', 'LC', 'LIMIT', 'LIMITED', 'LIMITED LIABILITY', 'LTD LIABILITY', 'LLC', 'LLP', 'LMTD', 'LP', 'LTD', 'MD', 'MDPA', 'OF', 'ON', 'PA', 'PARTNER', 'PARTNERS', 'PARTNERSHIP', 'PC', 'PROFESSIONAL', 'PTNR', 'THE']:
            if NoisyName.endswith(' '+ch):
                NoisyName=NoisyName[:-len(' '+ch)].strip()
                restart=True

    # Remove spaces
    NoisyName=NoisyName.replace(' ', '')

    NoisyName=re.sub('[^0-9a-zA-Z]+', '',NoisyName)

    return NoisyName


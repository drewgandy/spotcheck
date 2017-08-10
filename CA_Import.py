import re
from unidecode import unidecode
import MySQLdb as mdb
import logging

def Import_UCC_Data():
 #   import _mysql
 #   import CA_Denoise_Name
    import glob

    con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'], autocommit=True)
    counter = 0

    cur = con.cursor()
    con.autocommit(True)
    cur.execute("DROP TABLE IF EXISTS InitialFilingRecord")
    cur.execute("CREATE TABLE InitialFilingRecord(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), InitialFilingType VARCHAR(5), FilingDate VARCHAR(8), FilingTime VARCHAR(4), FilingStatus VARCHAR(1), LapseDate VARCHAR(8), PageCount VARCHAR(4))")
    cur.execute("DROP TABLE IF EXISTS BusinessDebtors")
    cur.execute("CREATE TABLE BusinessDebtors(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), Name VARCHAR(300), NonNoisyName VARCHAR(300), StreetAddress VARCHAR(110), City VARCHAR(64), State VARCHAR(32), ZipCode VARCHAR(15), ZipCodeExtension VARCHAR(6), CountryCode VARCHAR(3))")
    cur.execute("DROP TABLE IF EXISTS SecuredParties")
    cur.execute("CREATE TABLE SecuredParties(Id INT PRIMARY KEY AUTO_INCREMENT, InitialFilingNumber VARCHAR(14), Name VARCHAR(300), StreetAddress VARCHAR(110), City VARCHAR(64), State VARCHAR(32), ZipCode VARCHAR(15), ZipCodeExtension VARCHAR(6), CountryCode VARCHAR(3))")
 
    path = cfg.UCC_path
    for filename in glob.iglob(path + '*.txt'):
        print("OPENING: "+filename)
        #print(filename)
        f = open(filename, 'r', encoding='latin-1')
        skippedlines = 0
        initialFilings = 0
        BusinessDebtorCount = 0
        linecount = 0
        counter = 0
        for line in f:
            #print(line)
            t = line #.readline()

            # this can limit the number of records looped through
            #counter = counter + 1
            #if counter == 2:
                #con.commit()
                #con.close()
            #    print("BREAK EARLY##")
            #    break
            
            RecordCode = t[0:1]
            
            if RecordCode == "1": #Initial Filing Record
                #print "#################################################"
                #initialFilings=initialFilings+1
                #print("Filing Type: Initial Filing " + str(initialFilings))
                try:
                    cur.execute("INSERT INTO InitialFilingRecord(InitialFilingNumber, InitialFilingType, FilingDate, FilingTime, FilingStatus, LapseDate, PageCount) VALUES(%s, %s, %s, %s, %s, %s, %s)", (t[1:14].strip(),t[27:32].strip(),t[32:40].strip(),t[40:44].strip(),t[44:45].strip(),t[45:53].strip(),t[53:57].strip()))
                    con.commit()
                except (mdb.Error, mdb.Warning) as e:
                    print(e) 
                    return None

            if RecordCode == "2": #Business Debtor
                # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
                #BusinessDebtorCount=BusinessDebtorCount+1
                #print "Business Debtor " + str(BusinessDebtorCount)
                cur.execute("INSERT INTO BusinessDebtors(InitialFilingNumber, Name, NonNoisyName, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (t[1:14].strip(),t[27:327].strip(), DeNoiseName((t[27:327]).strip()), t[327:437].strip(),t[437:501].strip(),t[501:533].strip(),t[533:548].strip(),t[548:554].strip(),t[554:557].strip()))              
                con.commit()
    
#            if RecordCode == "3": #Personal Debtor - will skip
#                print("Personal Debtor skipped " + str(skippedlines))
            
            if RecordCode == "4": #Business Secured Party
                cur.execute("INSERT INTO SecuredParties(InitialFilingNumber, Name, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);", (t[1:14].strip(),t[27:327].strip(), t[327:437].strip(),t[437:501].strip(),t[501:533].strip(),t[533:548].strip(),t[548:554].strip(),t[554:557].strip()))
                con.commit()

            if RecordCode == "5":
                cur.execute("INSERT INTO SecuredParties(InitialFilingNumber, Name, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);", (t[1:14].strip(),t[77:127].strip() + " " + t[127:177].strip() + " " + t[27:77].strip() + " " + t[177:183].strip(), t[183:293].strip(),t[293:357].strip(),t[357:389].strip(),t[389:404].strip(),t[404:410].strip(),t[410:413].strip()))
                con.commit()

            #if RecordCode == "6": #Chang Filing (UCC3)
                #print "Initial Filing Number: "+t[2:15]
                #print "UCC3 Filing Number: "+t[15:27]
                #print "Change Filing Type: "+ChangeFilingType.get(t[27:32].strip(), "EMPTY")
            
            #if RecordCode == "7": #Collateral
                #skippedlines=skippedlines+1
                #print "collateral goes here - account for multiple lines of collateral, appending one after the next " + str(skippedlines)
        print("DONE with " + filename)
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

    con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'], autocommit=True)

    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS SOSCompanyList")
    cur.execute("CREATE TABLE SOSCompanyList(Id INT PRIMARY KEY AUTO_INCREMENT, CompNum VARCHAR(12), FormationDate VARCHAR(8), Status VARCHAR(1), Type VARCHAR(4), Name VARCHAR(350), NonNoisyName VARCHAR(350))")

    counter = 0
    
    path = cfg.Corp_path
    pct = 1
    portion= 186646
    for filename in glob.iglob(path + '*.TXT'):
        logging.debug(filename)
        f = open(filename, 'r', encoding='latin-1')
        logging.debug("OPEN: " + str(time.clock()))
        counter = 0
        linecount = 0
        for line in f:
            t = line #.readline()
            # this can limit the number of records looped through
            #counter = counter + 1
            #if counter == 5:
            #    break

            if filename.endswith('CORPMASTER.TXT'):
                # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
                cur.execute("INSERT INTO SOSCompanyList(CompNum, FormationDate, Status, Type, Name, NonNoisyName) VALUES(%s, %s, %s, %s, %s, %s);", (t[5:13].strip(), t[13:21].strip(), t[21].strip(), t[22:26].strip(), t[70:420].strip(), DeNoiseName(t[70:420])))
                con.commit()
                linecount=linecount+1

            if filename.endswith('LPMASTER.TXT'):
                # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
                cur.execute("INSERT INTO SOSCompanyList(CompNum, FormationDate, Status, Type, Name, NonNoisyName) VALUES(%s, %s, %s, %s, %s, %s);", (t[0:12].strip(), t[12:20].strip(), t[20].strip(), t[21].strip(), t[22:242].strip(), DeNoiseName(t[22:242])))
                con.commit()
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


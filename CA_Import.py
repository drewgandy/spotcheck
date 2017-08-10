def Import_UCC_Data():
    import MySQLdb as mdb
 #   import _mysql
    import CA_Denoise_Name
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
                cur.execute("INSERT INTO BusinessDebtors(InitialFilingNumber, Name, NonNoisyName, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)", (t[1:14].strip(),t[27:327].strip(), CA_Denoise_Name.DeNoiseName((t[27:327]).strip()), t[327:437].strip(),t[437:501].strip(),t[501:533].strip(),t[533:548].strip(),t[548:554].strip(),t[554:557].strip()))              
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
    import CA_Denoise_Name
    import time
    import glob
#    import os
    import config as cfg

    con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'], autocommit=True)

    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS SOSCompanyList")
    cur.execute("CREATE TABLE SOSCompanyList(Id INT PRIMARY KEY AUTO_INCREMENT, CompNum VARCHAR(12), FormationDate VARCHAR(8), Status VARCHAR(1), Type VARCHAR(4), Name VARCHAR(350), NonNoisyName VARCHAR(350))")

    counter = 0
    
    path = cfg.Corp_path
    for filename in glob.iglob(path + '*.txt'):
        print(filename)
        f = open(filename, 'r')
        print("OPEN: " + str(time.clock()))
        counter = 0
        linecount = 0
        for line in f:
            linecount=linecount+1
            t = line #.readline()
            
            # this can limit the number of records looped through
            #counter = counter + 1
            #if counter == 5:
                # break

            if filename.endswith('CORPMASTER.TXT'):
                # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
                cur.execute("INSERT INTO SOSCompanyList(CompNum, FormationDate, Status, Type, Name, NonNoisyName) VALUES(%s, %s, %s, %s, %s, %s);", (t[5:13].strip(), t[13:21].strip(), t[21].strip(), t[22:26].strip(), t[70:420].strip(), CA_Denoise_Name.DeNoiseName(t[70:420])))
                con.commit()
            if filename.endswith('LPMASTER.TXT'):
                # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
                cur.execute("INSERT INTO SOSCompanyList(CompNum, FormationDate, Status, Type, Name, NonNoisyName) VALUES(%s, %s, %s, %s, %s, %s);", (t[0:12].strip(), t[12:20].strip(), t[20].strip(), t[21].strip(), t[22:242].strip(), CA_Denoise_Name.DeNoiseName(t[22:242])))
                con.commit()

        f.close()
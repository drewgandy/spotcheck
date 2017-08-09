def CompareData_OnMySQLServer():
    import MySQLdb as mdb
    import MySQLdb.cursors
    import _mysql
    import sys
    import time

 #   import CA_Denoise_Name
 #   import CA_Import_UCC_Data
    print("COMPARE START")
    #con = mdb.connect('localhost', 'root', 'password', 'CA_UCC_testdb', cursorclass = MySQLdb.cursors.SSCursor);
    con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'], autocommit=True)
    print("COMPARING")
    cur = con.cursor()
    cur.execute("select * from TargetNames limit 100;")
    cur.execute("DROP TABLE IF EXISTS MatchedNames;")
    print( "Drop Table MatchedNames" + time.ctime())
    cur.execute("CREATE TABLE MatchedNames(InitialFilingNumber VARCHAR(14),  Name VARCHAR(300));")
    print("Create MatchedNames table " + time.ctime())
    cur.execute("INSERT INTO MatchedNames SELECT DISTINCT BusinessDebtors.InitialFilingNumber, SOSCompanyList.NonNoisyName FROM BusinessDebtors LEFT JOIN SOSCompanyList ON BusinessDebtors.NonNoisyName = SOSCompanyList.NonNoisyName WHERE SOSCompanyList.NonNoisyName IS NOT NULL;")
    print("Create UnMatchedNames table " + time.ctime())
    cur.execute("DROP TABLE IF EXISTS UnMatchedNames;")
    cur.execute("CREATE TABLE UnMatchedNames(InitialFilingNumber VARCHAR(14),  Name VARCHAR(300),  NonNoisyName VARCHAR(300));")
    cur.execute("INSERT INTO UnMatchedNames SELECT BusinessDebtors.InitialFilingNumber, BusinessDebtors.Name, BusinessDebtors.NonNoisyName FROM BusinessDebtors LEFT JOIN SOSCompanyList ON BusinessDebtors.NonNoisyName = SOSCompanyList.NonNoisyName WHERE SOSCompanyList.NonNoisyName IS NULL;")
    # OLD UNMATCHED NAMES QUERY.  QUERY ABOVE ADDS NONNOISYNAME COLUMN. 
#    cur.execute("INSERT INTO UnMatchedNames SELECT BusinessDebtors.InitialFilingNumber, BusinessDebtors.NonNoisyName FROM BusinessDebtors LEFT JOIN SOSCompanyList ON BusinessDebtors.NonNoisyName = SOSCompanyList.NonNoisyName WHERE SOSCompanyList.NonNoisyName IS NULL;")
    #    print("Create TargetNames table " + time.ctime())
    #    cur.execute("DROP TABLE IF EXISTS TargetNames")
    #    cur.execute("CREATE TABLE TargetNames(InitialFilingNumber VARCHAR(14),  Name VARCHAR(300))")
    #    cur.execute("INSERT INTO TargetNames SELECT UnMatchedNames.InitialFilingNumber, UnMatchedNames.Name FROM UnMatchedNames LEFT JOIN MatchedNames ON MatchedNames.InitialFilingNumber = UnMatchedNames.InitialFilingNumber WHERE MatchedNames.InitialFilingNumber IS NULL")


def CompareData_InPython():
    import MySQLdb as mdb
    import MySQLdb.cursors
    import sys

    con = mdb.connect('localhost', 'root', 'password', 'CA_UCC_testdb', cursorclass = MySQLdb.cursors.SSCursor);
    cur = con.cursor()
    conDb = mdb.connect('localhost', 'root', 'password', 'CA_UCC_testdb', cursorclass = MySQLdb.cursors.SSCursor);
    curDb = conDb.cursor()
    conSOS = mdb.connect('localhost', 'root', 'password', 'CA_UCC_testdb', cursorclass = MySQLdb.cursors.SSCursor);
    curSOS = conSOS.cursor()
    conMatch = mdb.connect('localhost', 'root', 'password', 'CA_UCC_testdb', cursorclass = MySQLdb.cursors.SSCursor);
    curMatch = conMatch.cursor()
    
    curMatch.execute("DROP TABLE IF EXISTS MatchedNames")
    curMatch.execute("DROP TABLE IF EXISTS UnMatchedNames")
    cur.execute("CREATE TABLE MatchedNames(InitialFilingNumber VARCHAR(14),  Name VARCHAR(300))")
    cur.execute("CREATE TABLE UnMatchedNames(InitialFilingNumber VARCHAR(14),  Name VARCHAR(300))")

    
    cur.execute("SELECT InitialFilingNumber, InitialFilingType, FilingStatus FROM InitialFilingRecord")
    row = cur.fetchone()
    #con.use_result()
    rowcount = 0
    MissingDebtors=0
    while row is not None:
        if row[1] == "1" or row[1] == "2":
            curDb.execute("SELECT * FROM BusinessDebtors WHERE InitialFilingNumber = %s", [row[0]])
            results = curDb.fetchall()
            #print "***NUMBER OF DEBTORS: "+str(len(results))
            if len(results)==0:
                MissingDebtors = MissingDebtors + 1
                print("Missing Debtors" + str(MissingDebtors) + " " + row[0])
            for Db in results:
                #print "NonNoisy Name " + Db[3]
                curSOS.execute("SELECT * FROM SOSCompanyList WHERE NonNoisyName = %s", [Db[3]])
                SOSresults = curSOS.fetchall()
                #print "SOS Results "
                #print SOSresults
                if len(SOSresults)>0:
                    curMatch = conMatch.cursor()
                    curMatch.execute("INSERT INTO MatchedNames(InitialFilingNumber, Name) VALUES(%s, %s);", (Db[1],Db[2]))
                    conMatch.commit()
                else:
                    curMatch = conMatch.cursor()
                    curMatch.execute("INSERT INTO UnMatchedNames(InitialFilingNumber, Name) VALUES(%s, %s);", (Db[1],Db[2]))
                    conMatch.commit()
                    print("NO MATCH " + Db[2])
                    

        row = cur.fetchone()
        #rowcount = rowcount + 1
        #if rowcount == 100:
            #print "100 results - ending"
            #sys.exit()
    cur.close()
    curDb.close()
    conn.close()
def Import_Corp_Data():
    import MySQLdb as mdb
#    import _mysql
    import CA_Denoise_Name
    import time
    import glob
#    import os

    con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'], autocommit=True)

    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS SOSCompanyList")
    cur.execute("CREATE TABLE SOSCompanyList(Id INT PRIMARY KEY AUTO_INCREMENT, CompNum VARCHAR(12), FormationDate VARCHAR(8), Status VARCHAR(1), Type VARCHAR(4), Name VARCHAR(350), NonNoisyName VARCHAR(350))")

    counter = 0
    
    path = cfg.Corp_path
    for filename in glob.glob(path, '*.TXT'):
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
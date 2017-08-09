import MySQLdb as mdb
import MySQLdb.cursors
#import _mysql
import CA_Denoise_Name

print("COMPARE START")
#con = mdb.connect('localhost', 'root', 'password', 'CA_UCC_testdb', cursorclass = MySQLdb.cursors.SSCursor);
con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'], autocommit=True)
print("DONE ")
cur = con.cursor()
cur.execute("update BusinessDebtors set NonNoisyName = %d;", CA_Denoise_Name.DeNoiseName(%d))
    #cur.execute("DROP TABLE IF EXISTS MatchedNames")


#for row in result:
#    print(row)

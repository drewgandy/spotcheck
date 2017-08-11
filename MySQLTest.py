import MySQLdb as mdb
import MySQLdb.cursors
import config as cfg

print("COMPARE START")
#con = mdb.connect('localhost', 'root', 'password', 'CA_UCC_testdb', cursorclass = MySQLdb.cursors.SSCursor);
con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'], autocommit=True)
cur = con.cursor()
#column=[]
#columns=""
#cur.execute("update BusinessDebtors set NonNoisyName = %d;", CA_Denoise_Name.DeNoiseName(%d))
tables = ['']
cur.execute('SHOW columns FROM BusinessDebtors')
columns = cur.fetchall()
for column in columns:
    print(column[0])
    if column[0] != 'Id':
        sql = "UPDATE BusinessDebtors SET %s = TRIM(%s)"
        cur.execute(sql % (column[0], column[0]))

#print(cur.description[0])
#for row in result:
#    print(row)
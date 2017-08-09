from unidecode import unidecode

s="Montréal, über, 12.89, Mère, Françoise, ñoël, 889"
#s.encode("ascii")  #doesn't work - traceback
t=unidecode(s)
t.encode("ascii")  #works fine, because all non-ASCII from s are replaced with their equivalents
print(t)  #gives: 'Montreal, uber, 12.89, Mere, Francoise, noel, 889'

import config as cfg
import MySQLdb as mdb

con = mdb.connect(host=cfg.mysql['host'], port=cfg.mysql['port'], user=cfg.mysql['user'], passwd=cfg.mysql['passwd'], db=cfg.mysql['db'], autocommit=True)

cur = con.cursor()
con.autocommit(True)
data = cur.execute("SELECT * from ShortList limit 5")
data = cur.fetchall()
for row in data:
    print(row)

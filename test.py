from unidecode import unidecode
import config as cfg
import glob
import logging

s="Montréal, über, 12.89, Mère, Françoise, ñoël, 889"
#s.encode("ascii")  #doesn't work - traceback
t=unidecode(s)
t.encode("ascii")  #works fine, because all non-ASCII from s are replaced with their equivalents
print(t)  #gives: 'Montreal, uber, 12.89, Mere, Francoise, noel, 889'
total_linecount =0
#logger = logging.getLogger ()
#logger.setLevel(logging.DEBUG)
logging.basicConfig(level=logging.DEBUG)
logging.debug("RUNNING")
path = cfg.UCC_path
for filename in glob.iglob(path + '*.txt'):
    logging.info('OPENING: '+filename)
    #logging.debug(filename)
    f = open(filename, 'r', encoding='latin-1')
    file_linecount = 0
    for line in f:
        file_linecount=file_linecount+1
    f.close()
    total_linecount = total_linecount + file_linecount
    logging.info("Linecount of: "+filename +" = " + str(file_linecount))
    logging.info("Total Linecount =" + str(total_linecount))
    
    
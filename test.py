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
#logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(filename='UCC_import.log',format='%(asctime)s %(message)s', level=logging.DEBUG)
logging.debug("RUNNING")
logging.info("####################################")
path = cfg.UCC_path
logging.info(glob.glob(path + '*.txt'))
correctifr = 0
for filename in glob.iglob(path + '*.txt'):
    logging.info('OPENING: '+filename)
    #logging.debug(filename)
    f = open(filename, 'r', encoding='latin-1')
    file_linecount = 0
    for line in f:
        file_linecount=file_linecount+1
        RecordCode = line[0:1]
        if RecordCode == '1' and len(line.rstrip('\n')) != 650:
            logging.info("@@"+line.rstrip('\n')+"@@")
            logging.info("LENGTH " + str(len(line.rstrip('\n'))))
        if RecordCode =='1' and line[0:15] len(line.rstrip('\n'))==650:
            correctifr = correctifr + 1
    f.close()
    total_linecount = total_linecount + file_linecount
    logging.info("Linecount of: "+filename +" = " + str(file_linecount))
    logging.info("Total Linecount =" + str(total_linecount))
    logging.info("Total IFR =" + str(correctifr))
    
    
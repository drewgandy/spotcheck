
def module(varname=''):
    print("*"+str(varname)+"*")
import CA_Import
import logging
import config as cfg


logging.basicConfig(level=logging.INFO)


module()
#CA_Import.Import_UCC_Data_infile(9)
a='123456789 '
print(a.strip().isdigit())
if a.strip().isdigit() is True and len(a.strip()) > 8:
    print(a)


linecount = 0
f = open('/home/ubuntu/code/data/CA/UCC/pd.dat', 'r',  encoding='latin-1')#, encoding='latin-1')
for t in f:
    linecount = linecount + 1
    if linecount == 230302 or linecount == 242926:
        logging.info("@@"+t+"@@")
        logging.info("length: " + str(len(t)))
        logging.info("truncated length: " + str(len(t.strip())))
        text = t
        print(isinstance(t,str))
        s=t.encode('utf-8')
        print("##")
        print(s)
            
f.close
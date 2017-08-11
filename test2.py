import logging
import config as cfg

logging.basicConfig(level=logging.INFO)
logging.warning('Watch out!')  # will print a message to the console
logging.info('I told you so')  # will not print anything
path = cfg.UCC_path
f = open(path+"ifr.dat", 'r')
linecount=0
for line in f:
    print(line)
    t=line
    print("*"+t[1:15]+ t[27:57]+"*")
    print(str(len(line)))
    linecount = linecount + 1
    if linecount == 2:
            break
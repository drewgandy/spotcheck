import re
from unidecode import unidecode

    # De-Noisify Debtor's Name (remove punctuation marks, business abbreviations, etc.)
def DeNoiseName(NoisyName):
    # strip spaces from name and make uppercase
    NoisyName = NoisyName.strip().upper()
    
    NoisyName = unidecode(NoisyName)

    # remove punctuation and replace with space, but then strip spaces if at end of line (spaces at end of line were causing, e.g. "Inc." not to be found at end of line because period replaced with space
    for ch in ['\"', "\'", '[', ']', '{', '}', '(', ')', ':', ',', '!', '<', '>', '?', ';', '\\', '//', '.','-','`','~']:
        if ch in NoisyName:
            NoisyName=NoisyName.replace(ch," ").strip()
    
    # Replace "&" with "AND" (NOTE: CONFIRMED THAT CA SOS DOES THIS IN SEARCH LOGIC)
    NoisyName=NoisyName.replace('&', 'AND')
    
    # ignore "THE" at beginning of name
    if NoisyName.startswith('THE '):
        NoisyName = NoisyName[4:]
    
    # combine single letters together (e.g. "L L C" becomes "LLC") NOTE: I'M NOT POSITIVE CA SOS DOES THIS
    NoisyName = re.sub(r"\b(\w) (?=\w\b)", r"\1", NoisyName)
            
    # remove words and abbreviations at the end of a name that indicate the existence or nature of an organization.  This loops until all relevant words are removed. NOTE: I'M NOT POSITIVE CA SOS REMOVES 'A CALIFORNIA...'
    # NOTE: confirmed that CA SOS does also remove California from 'California LLC' etc.
    restart = True
    while restart == True:
        restart = False #reset restart flag
        for ch in ['A', 'A CALIFORNIA', 'AKA', 'AN', 'AND', 'ASSN', 'ASSOC', 'ASSOCIATES', 'ASSOCIATION', 'ASSOCS', 'AT', 'CO', 'COMPANIES', 'COMPANY', 'COMPANYS', 'COOP', 'COOPERATIVE', 'CORP', 'CORPORATION', 'DBA', 'DIV', 'DIVISION', 'FDBA', 'FKA', 'FOR','IN', 'INC', 'INCORPORATED', 'IS', 'LC', 'LIMIT', 'LIMITED', 'LIMITED LIABILITY', 'LTD LIABILITY', 'LLC', 'LLP', 'LMTD', 'LP', 'LTD', 'MD', 'MDPA', 'OF', 'ON', 'PA', 'PARTNER', 'PARTNERS', 'PARTNERSHIP', 'PC', 'PROFESSIONAL', 'PTNR', 'THE']:
            if NoisyName.endswith(' '+ch):
                NoisyName=NoisyName[:-len(' '+ch)].strip()
                restart=True

    # Remove spaces
    NoisyName=NoisyName.replace(' ', '')

    NoisyName=re.sub('[^0-9a-zA-Z]+', '',NoisyName)

    return NoisyName




def ShowStringDifferences(a,b):
    import difflib

    print('{} => {}'.format(a,b))
    for i,s in enumerate(difflib.ndiff(a, b)):
        if s[0]==' ': continue
        elif s[0]=='-':
            print(u'Delete "{}" from position {}'.format(s[-1],i))
        elif s[0]=='+':
            print(u'Add "{}" to position {}'.format(s[-1],i))
    print()
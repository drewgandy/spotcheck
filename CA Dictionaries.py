InitialFilingType = {
    "1" : "Financing Statement",
    "2" : "Public Finance Transaction",
    "3" : "Manufactured Home Transaction",
    "4" : "Transmitting Utility",
    "5" : "Federal Tax Lien",
    "6" : "Federal Estate Tax Lien",
    "7" : "Pension Benefit Lien",
    "8" : "State Tax Lien",
    "9" : "Judgment Lien"
}
FilingStatus = {
    "A" : "Unlapsed",
    "L" : "Lapsed",
    "D" : "Administrative Deletes",
    "E" : "Expunged"
}

ChangeFilingType = {
    "2" : "Financing Statement - Filing Officer Statement",
    "3" : "Financing Statement - Full Master Amendment",
    "4" : "Financing Statement - Full Master Assignment",
    "5" : "Financing Statement - Termination",
    "6" : "Financing Statement - Continuation",
    "7" : "Financing Statement - Assignment",
    "8" : "Financing Statement - Amendment",
    "9" : "Financing Statement - Correction Statement",
    "10" : "Financing Statement - Court Order",
    "11" : "Financing Statement - Court Order No Change",
    "283" : "State Tax Lien - Termination"
}
    
    print("Initial Filing Number: "+t[1:14])
    print("Initial Filing Type: "+InitialFilingType.get(t[27:32].strip(), {}))
    print("Filing Date: "+t[32:40])
    print("Filing Status: "+FilingStatus.get(t[44:45]))
    print("Lapse Date: "+t[45:53])
    print("Page Count: "+t[53:57])

    if RecordCode == "2": #Business Debtor
        with con:
            cur.execute("INSERT INTO BusinessDebtors(InitialFilingNumber, Name, StreetAddress, City, State, ZipCode, ZipCodeExtension, CountryCode) VALUES(%s, %s, %s, %s, %s, %s, %s, %s);", (t[1:14].strip(),t[27:327].strip(),t[327:437].strip(),t[437:501].strip(),t[501:533].strip(),t[533:548].strip(),t[548:554].strip(),t[554:557].strip()))
        
        
        print("Business Debtor Name: "+t[27:327].strip())
        print("Business Debtor Street Address: "+t[327:437].strip())
        print("Business Debtor City: "+t[437:501].strip())
        print("Business Debtor State: "+t[501:533].strip())
        print("Business Debtor Zip Code: "+t[533:548].strip())
        print("Business Debtor Zip Code Extension: "+t[548:554].strip())
        print("Business Debtor Country Code: "+t[554:557].strip())



# create dict workingsymbols to include symbols minus the current libraries symbols

def getWorkingSymbols(oclcsymbols, library, members):
    # for library split space
    workingsymbols = dict(oclcsymbols)

    for sym in members[library]['symbols'].split(" ") : # might be one or more space delimited list NHM MVA
        try:
            del workingsymbols[sym.upper()]
            #print "Removed " + sym
        except KeyError:
            #print "sym is " + sym
            # print "sym.upper is " + sym.upper()
            print("Error removing " + sym.upper() + " from workingsymbols")
            sys.exit # this actually gives an error as sys not in scope
    
    return workingsymbols
  

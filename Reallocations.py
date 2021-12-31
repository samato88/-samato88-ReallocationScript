
import sys
from sys import argv
import time
import os, shutil
import pandas as pd
import pandas.io.formats.excel
from _collections import defaultdict
import numpy as np
import random
import getMembers
from openpyxl.styles import Font, Fill, NamedStyle, Border, Side, PatternFill, Alignment

if len(argv) < 2:
  print("Must provide file name to process!")
  sys.exit()

## UPDATE reports_dir variable before running. Note that script DELETES anything in that dir when starting
##  this script last run on live data Dec 28, 2021
##  since then have run with test reports_dir as output to try to fix summary report which had wrong sum # allocated
##  python3 Reallocations.py  ../2021/2021Input/201906-202112-ToReallocate.WithHoldings.3rdFullRun.csv 

# input file must have these headers (these are created by default by script that looks up data in OCLC api - pySharedPrint):
### "Symbol", "oclcNumber", "Current OCN", "merged_OCNs", "Title", "# EAST Holdings", "EAST Holdings Symbols", 
### "# US WorldCat Holdings", "# EAST Retained", "EAST Retained Symbols", "status"

# GOAL:  Report out :
# excel files with disposition of requests, any uniques, and any requests to retain for EAST
# also makes a summary sheet - Take UNIQUE tab and ADDED TO TRACKING SHEET 
#
# TO DO
#  -- make flowchart/documentation of what script does? or in conjunction with script that builds input file

# NOTE "XlsxWriter is designed only as a file writer. It cannot read or modify an existing Excel file." - https://xlsxwriter.readthedocs.io/faq.html#q-can-xlsxwriter-use-an-existing-excel-file-as-a-template
# so have to use openpyxl if want to ADD a sheet to existing file. This means reworking formatting headers. 

#testfile:
#/Users/samato/Dropbox/EAST/OCLC/Reallocation/sampleInput.csv
# (venv) : python3 Reallocations.py /Users/samato/Dropbox/EAST/OCLC/Reallocation/sampleInput.csv

def columnHeader(sheet): # in theory could alternatively create named stye
    for cell in sheet["1:1"]: # set header row style
        cell.font = Font(name="Calibri", size=14, bold=True)
        cell.alignment = Alignment(vertical="top", wrap_text=True)
        cell.fill = PatternFill(fill_type='solid', start_color='6495ED', end_color='6495ED')
        
    if sheet.title == "Summary by Library" :
        sheet.column_dimensions['A'].width = 18
        sheet.column_dimensions['B'].width = 10
        sheet.column_dimensions['C'].width = 16
        sheet.column_dimensions['D'].width = 16
           
    if sheet.title == "Request Retain for EAST" or sheet.title == "All Reallocs":
        sheet.column_dimensions['A'].width = 11
        sheet.column_dimensions['B'].width = 14
        sheet.column_dimensions['C'].width = 15
        sheet.column_dimensions['D'].width = 16
        sheet.column_dimensions['E'].width = 16
        sheet.column_dimensions['F'].width = 30
        sheet.column_dimensions['G'].width = 12
        sheet.column_dimensions['H'].width = 14
        sheet.column_dimensions['I'].width = 15
        sheet.column_dimensions['J'].width = 20
        sheet.column_dimensions['K'].width = 15

    if sheet.title == "Disposition of Requests" or sheet.title == "Unique" or sheet.title == "All Disposition" or sheet.title == "All Unique":
        sheet.column_dimensions['A'].width = 11
        sheet.column_dimensions['B'].width = 14
        sheet.column_dimensions['C'].width = 15
        sheet.column_dimensions['D'].width = 16
        sheet.column_dimensions['E'].width = 16
        sheet.column_dimensions['F'].width = 30
        sheet.column_dimensions['G'].width = 12
        sheet.column_dimensions['H'].width = 14
        sheet.column_dimensions['I'].width = 15
        sheet.column_dimensions['J'].width = 20
        sheet.column_dimensions['K'].width = 15        
        sheet.column_dimensions['L'].width = 14
        sheet.column_dimensions['M'].width = 14

def main():
    starttime = time.time()
    #reports_dir = "/Users/samato/Dropbox/EAST/OCLC/Reallocation/2021/2021Reports/"
    reports_dir = "/Users/samato/Dropbox/EAST/OCLC/Reallocation/2021/test/"

    for files in os.listdir(reports_dir): # clear out directory for this run (I think this is okay to do)
        path = os.path.join(reports_dir, files)
        try:
            shutil.rmtree(path)
        except OSError:
            os.remove(path)
    
    
    pd.io.formats.excel.ExcelFormatter.header_style = None # going to reset excel header format later on, turn off default here https://stackoverflow.com/questions/36694313/pandas-xlsxwriter-format-header/55666917

    script, myfile = argv

    oclcsymbols = {}
    workingsymbols = {}
    countTotal = 0
    prevlibrary = ''
    
    memtype = 'Monographs'
    libnames = getMembers.getMembers(memtype) # load up oclcsymbol =>libname hash
  
    df = pd.read_csv(myfile, keep_default_na=False) 
    #df = pd.read_csv(myfile) 

    #print(df.duplicated()) # lists all rows and if duplicate of previous rows
    df.drop_duplicates(inplace = True) # removes any duplicate rows, might want to rethink and just check dup OCLC/Symbol

    #defaultdict(list) is stackexchange hack to allow appending to new key, see:
    #https://stackoverflow.com/questions/49881570/python-dictionaries-appending-arrays-to-a-dictionary-for-a-specific-key

    symbol_dict = {} # all symbols(key) that requested reallocation and count of how many requested, actually now using for summary stats too
    request_retain = defaultdict(list) # requests - request to(key), requestee,  submitted ocn, current ocn, merged ocn, title, held by, also retained by
    unique_to_EAST = defaultdict(list) # uniques - submitted ocn, current ocn, merged ocn, requestee, title
    disposition    = defaultdict(list) # request and what happened to it - requestee(key), disposition, submitted ocn, current ocn, merged ocn, title, held by, retained by
    
    for x in df.index: # for each line in the input file
        
        if not df.loc[x, "Symbol"] : # checking that symbol. not blank, skip any blanks, shouldn't be any!
            #print(type(df.loc[x, "Symbol"]))   
            continue

        sym = df.loc[x, "Symbol"].strip() # strip removes leading/trailing whitespace  #print("*" + df.loc[x, "Symbol"] + "*")
        socn = df.loc[x, "oclcNumber"]
        cocn = df.loc[x, "Current OCN"]
        mocn = df.loc[x, "merged_OCNs"]
        title = df.loc[x, "Title"]
        numberEASTHoldings = df.loc[x, "# EAST Holdings"]
        eastHolders = df.loc[x, "EAST Holdings Symbols"]
        worldCat = df.loc[x, "# US WorldCat Holdings"]
        numberEASTRetained = df.loc[x, "# EAST Retained"]
        eastRetainers = df.loc[x, "EAST Retained Symbols"]
        status = df.loc[x, "status"]
        
        syminholderslist = ""
        syminretentionslist = ""
        
        if sym in symbol_dict: # have seen this symbol already, increment [sym]: [libname, # submitted, # request retain]
            symbol_dict[sym][1] += 1
            #symbol_dict[sym] += 1
        else: 
            symbol_dict.update({sym : [libnames[sym], 1, 0]})

        #if np.isnan(cocn): # NaN, not a valid OCN, report and move on
        if not cocn: # cocn is empty string, report and move on
            #print("HERE line: ", x)
            cocn = "Invalid OCN Submitted"
            if not (title):
                title = ""
            disposition[sym].append([sym, socn, "Invalid OCN", "", "", title, "", "", "", "", "", "", ""]) 
            continue
        else: # make these ints - were floats, e.g. 650.0 and make list type for holders and retainers
            cocn = int(cocn)
            #print("line ", x, " ", numberEASTRetained)
            numberEASTHoldings = int(numberEASTHoldings)
            numberEASTRetained = int(numberEASTRetained)
            
            try:
                retainerslist = list(eastRetainers.split(",")) ## make list of east retainers
            except:
                retainerslist = []
            try:
                holderslist =  list(eastHolders.split(",")) ## make list of east holders
            except:
                holderslist = []
            
            if (numberEASTRetained > 0) and (len(retainerslist) != numberEASTRetained): # just a little sanity checking
                print("SCRIPT ERROR - numberEASTRetained does not equal length of retainerslist")

            if (numberEASTHoldings > 0) and (len(holderslist) != numberEASTHoldings):
                print("SCRIPT ERROR - numberEASTHoldings does not equal length of holderslist")

        if sym in holderslist: # check if sym in holders list, if so remove and decrement holdings
            syminholderslist = "YES" # doing this separately from below so can flag things that still have holdings set
            holderslist.remove(sym)
            numberEASTHoldings -= 1  
            
        if sym in retainerslist:  # Remove sym from eastRetainers - note if still has retention on it
            syminretentionslist = "YES"
            retainerslist.remove(sym)
            numberEASTRetained -=1
            
        if numberEASTHoldings == 0: # unique to EAST, write to disp and unique
            disposition[sym].append([sym, socn, "unique", cocn, mocn, title, numberEASTHoldings, ','.join(holderslist), numberEASTRetained, ','.join(retainerslist), worldCat, syminholderslist, syminretentionslist]) 
            unique_to_EAST[sym].append([sym, socn, "unique", cocn, mocn, title, numberEASTHoldings, ','.join(holderslist), numberEASTRetained, ','.join(retainerslist), worldCat, syminholderslist, syminretentionslist])
            continue
        
        for holder in holderslist: # remove all retainers from holders list, recalculate number of holders
            if holder in retainerslist:
                holderslist.remove(holder)
                numberEASTHoldings -=1 # calculate number of spare copy holders, i.e. not retainers
                    
        if numberEASTHoldings == 0: # no spare copies in EAST, all holders were retainers, write to disp 
            disposition[sym].append([sym, socn, "no unretained copies in EAST", cocn, mocn, title, numberEASTHoldings, ','.join(holderslist), numberEASTRetained, ','.join(retainerslist), worldCat, syminholderslist, syminretentionslist]) 
            #unique_to_EAST[sym].append([sym, socn, "no unretained copies in EAST", cocn, mocn, title, numberEASTHoldings, ','.join(holderslist), numberEASTRetained, ','.join(retainerslist), worldCat, syminholderslist, syminretentionslist])
            continue

        if numberEASTRetained > 4: # already have enough of these, write to disposition
            disposition[sym].append([sym, socn, "over retained", cocn, mocn, title, numberEASTHoldings, ','.join(holderslist), numberEASTRetained, ','.join(retainerslist), worldCat, syminholderslist, syminretentionslist]) 
            continue
        
        #if numberEASTRetained > 0 and numberEASTRetained < 5: 
        if numberEASTRetained < 5: # check for surplus holdings copies,
            #print("Assigning realloc HERE")
            if len(holderslist) == 0:
                print("Script Logic Error, script line 168ish, data line: " + str(x))
            #randomly pick a remaining holder and write to both disp and realloc requests - make sure to key correct symbol for realloc request
            realloc_lib = random.choice(holderslist) # a better allocation method would be to look at ALL holders across ALL requests and allocate
            disposition[sym].append([sym, socn, realloc_lib, cocn, mocn, title, numberEASTHoldings, ','.join(holderslist), numberEASTRetained, ','.join(retainerslist), worldCat, syminholderslist, syminretentionslist]) 
            request_retain[realloc_lib].append([realloc_lib, socn, sym, cocn, mocn, title, numberEASTHoldings, ','.join(holderslist), numberEASTRetained, ','.join(retainerslist), worldCat]) 
           
        else:
            print("This should never happen - assignment logic busted - Line: ",  str(x),  " OCN: " , socn)
            disposition[sym].append([sym, socn, "PROCESSING SCRIPT ERROR", cocn, mocn, title, numberEASTHoldings, ','.join(holderslist), numberEASTRetained, ','.join(retainerslist), worldCat, syminholderslist, syminretentionslist]) 

    ##### this marks the end of processing the input file of retention reallocation requests with their oclc holdings



    disp_column_names    = ["Symbol", "Sumbitted OCLC #", "Disposition", "WorldCat Current OCLC #", "Merged OCLC #s", "Title", "# EAST Holdings", "EAST Holders", "# EAST Retentions", "EAST Retainers", "# WorldCat Holdings", "Symbol Holdings Set", "Symbol Retention Set"]
    unique_column_names  = ["Symbol", "Sumbitted OCLC #", "Disposition", "WorldCat Current OCLC #", "Merged OCLC #s", "Title", "# EAST Holdings", "EAST Holders", "# EAST Retentions", "EAST Retainers", "# WorldCat Holdings", "Symbol Holdings Set", "Symbol Retention Set"]
    realloc_column_names = ["Symbol", "Requested OCLC #", "Requesting Library", "WorldCat Current OCN", "Merged OCLC #s", "Title", "# EAST Holdings", "EAST Holders", "# EAST Retentions", "EAST Retainers", "# WorldCat Holdings"]
    #print(type(disposition)) # <class 'collections.defaultdict'>
    #print(type(disposition['NKF'])) # <class 'list'>
    allDisposition = pd.DataFrame(columns=disp_column_names)
    allUnique  = pd.DataFrame(columns=unique_column_names)
    allRealloc = pd.DataFrame(columns=realloc_column_names)

    headerstyle = {"font_name": "Calibri", "font_size": "14", "bold": "True", "bg_color": "#6495ED", "text_wrap": "True", "valign": "Top"}

    for lib in symbol_dict: # make directories - this is every symbol that submitted a realloc request
        if not os.path.isdir(reports_dir + libnames[lib]):
            os.mkdir(reports_dir + libnames[lib])
    for lib in request_retain: # make directories - this is every symbol of whom we are asking that we retain something
        if not os.path.isdir(reports_dir + libnames[lib]):
            os.mkdir(reports_dir + libnames[lib])
        
    for lib in symbol_dict: # foreach lib in the disposition report - this does their unqiques and disposition file
        dispdf   = pd.DataFrame(disposition[lib], columns=disp_column_names)  
        uniquedf = pd.DataFrame(unique_to_EAST[lib], columns=disp_column_names)  

        # print out to files for lib 
        excelfilename = reports_dir + libnames[lib] + "/" + lib + "-EAST_Reallocation_Report.xlsx"
        writer = pd.ExcelWriter(excelfilename, engine="openpyxl")
        workbook  = writer.book

        if not dispdf.empty:  ## only write out if not empty. disp should always have data, unique may not
            dispdf.to_excel(writer, sheet_name="Disposition of Requests", index=False)
            columnHeader(workbook["Disposition of Requests"])

        if not uniquedf.empty:
            uniquedf.to_excel(writer, sheet_name="Unique", index=False)
            columnHeader(workbook["Unique"])

        writer.save()

        allDisposition = allDisposition.append(dispdf, ignore_index = True)
        allUnique = allUnique.append(uniquedf, ignore_index = True)
    
    for lib in request_retain: # foreach lib in the request retain for EAST report - this is what we want them to consider retaining
        # updated symbol_dict to include number requested retained for summary report
        if lib in symbol_dict: # have seen this symbol already, increment # request retain: [libname, # submitted, # request retain]
            symbol_dict[lib][2] = len(request_retain[lib]) #  len(request_retain[lib]) is total number reallocated to lib
        else: 
            symbol_dict.update({lib : [libnames[lib], 0, len(request_retain[lib])]}) #(len(request_retain[lib])) is total number reallocated to lib
        
        reallocdf = pd.DataFrame(request_retain[lib], columns=realloc_column_names)  
        excelfilename = reports_dir + libnames[lib] + "/" + lib + "-EAST_Reallocation_Report.xlsx"
        
        ## check if file exists - if so open in append
        if os.path.exists(excelfilename):
            with pd.ExcelWriter(excelfilename, engine="openpyxl", mode="a") as writer:  
                workbook  = writer.book
                if not reallocdf.empty: # just checking, should actually never be empty if you've made this far            
                    reallocdf.to_excel(writer, sheet_name='Request Retain for EAST', index=False)
                    columnHeader(workbook['Request Retain for EAST'])
                    writer.save()
        else:
            writer = pd.ExcelWriter(excelfilename, engine="openpyxl")
            workbook  = writer.book        
            if not reallocdf.empty: # just checking, should actually never be empty if you've made this far            
                reallocdf.to_excel(writer, sheet_name='Request Retain for EAST', index=False)
                columnHeader(workbook['Request Retain for EAST'])
                writer.save()
       
        allRealloc = allRealloc.append(reallocdf, ignore_index = True)

    # Summary reports  
    allSummary = pd.DataFrame(symbol_dict).transpose(copy=True) # 
    allSummary.reset_index(inplace=True) # make the index key (symbol) a column
    allSummary.columns = ['Symbol', 'Library', 'Submitted for Reallocation', 'Assigned for Retention'] # give columns names
    allSummary.sort_values(by=['Library'], inplace=True) # sort by library name
    allSummary = allSummary.reindex(columns=["Library", "Symbol", "Submitted for Reallocation", "Assigned for Retention"]) # reorder so library name in column 1    

    with pd.ExcelWriter(reports_dir + 'All_Summary.xlsx', engine="openpyxl") as writer: # write summary to excel
        allSummary.to_excel(writer, sheet_name='Summary by Library', index=False)
        allDisposition.to_excel(writer, sheet_name='All Disposition', index=False)
        allUnique.to_excel(writer, sheet_name='All Unique', index=False)
        allRealloc.to_excel(writer, sheet_name='All Reallocs', index=False)
        workbook  = writer.book

        #ws = workbook['Summary by Library'] 
        columnHeader(workbook['Summary by Library'])
        columnHeader(workbook['All Disposition'])
        columnHeader(workbook['All Unique'])
        columnHeader(workbook['All Reallocs'])

    print(allSummary)
        
    endtime = time.time()
    runtime =  int(endtime - starttime)
    print("Runtime: " + str(runtime) + " seconds")


if __name__ == '__main__':
    main()

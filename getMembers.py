from __future__ import print_function #https://docs.python.org/2/reference/simple_stmts.html#future
import pickle
import os.path
import re
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
#from inspect import getmembers

# set member type in main before running

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range  of members spreadsheet.
MEMBER_SPREADSHEET_ID = '1ceiwwnXj-Gv3W9tXXppAlx2nh4kX0SqM3z_-cDOo1Xg'
MEMBER_RANGE_NAME = 'Members!A:S'

def getMembers(memtype):

    """get member info from google sheet
       create list of oclc symbols based on member typ
       create dict keyed on members
       create dict keyed on oclc symbol which includes inst name and operational contact
    """
    #print("MEMTYPE:" + memtype)
    members = dict()
    oclcsymbols = dict()
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server()
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=MEMBER_SPREADSHEET_ID,
                                range=MEMBER_RANGE_NAME).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        for row in values:
            #print('%s, %s, %s, %s' % (row[0], row[5], row[12], row[18]))
            if row[0] == 'Inst_ID': # skip 1st row
                continue 
            # print(members[row[0]])
            members[row[0]] = {'valscore': row[2], 'name': row[5], 'mtype': row[12], 'symbols': row[18] } # using inst id as key 

    # fix union college (cohort 2) name so that they end up in same files as union college
    members['13946']['name']="Union College"
    
    for mem in members:
        #print (members[mem]['name'])
        # if mtype has memtype load up oclcsymbols - has to be split on space
        # push these onto oclcsymbols members[mem]['symbols'].split(' ')
        #s = "Name1=Value1;Name2=Value2;Name3=Value3"
        #>>> dict(item.split("=") for item in s.split(";"))
       
        if re.search(memtype, members[mem]['mtype']) :   # if re.search(pattern, text)
            #print (members[mem]['symbols'])
            for sym in members[mem]['symbols'].split(" ") :
                libname = members[mem]['name'] 
                libname =libname.replace("(", "") # rm (, ),' and sub space with _   e.g. St. Mary's to St_Marys
                libname = libname.replace(")", "")
                libname =libname.replace("'", "")
                libname =libname.replace(".", "")                  
                libname =libname.replace(" ", "_")
                #print(libname)
                oclcsymbols[sym.upper()] = libname # return keyed on symbol = name
                
    #print(', '.join(oclcsymbols))

    return oclcsymbols;

if __name__ == '__main__': # sometimes need to run this from the command line to refresh authorization
    getMembers("Monographs","","") # need to remember what those last two are for

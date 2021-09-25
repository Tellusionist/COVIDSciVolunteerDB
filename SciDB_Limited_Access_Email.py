# # Daily SciDB Limited Access Validator and Emailer #
# 
#  - Download the latest Volunteer Response data
#    - https://docs.google.com/spreadsheets/d/1WrHA3BQKB7-0_EP7SZY1mVWLV0krkkM-6pvv0yHag5o/edit?ts=5e88d0fb#gid=1304812603)
#  - Download the latest configuration 
#    - https://docs.google.com/spreadsheets/d/1cFCs3XzvrSKkSdW5elsxxZuPgDheSGUaMUjWvYqCtmU/edit#gid=1042414742
#  - Check against day frequency for that user if enough time has passed since previous run
#  - Compare against previous output to see if new volunteers are found, email an updated list
#    - Don't send an email if someone was removed, but update the last run date
#    - Update the main table with the temp table if ther were new volunteers and send email
#  - Update run nots to the configuration file
#  - Add column [Unique ID] to "All Volunteers" backup if Volunteers_Subset_DB is deleted (don't email or upload)

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import logging
from sci_access import get_zips, parse_dts, upload_aws
from sci_email import email_results, heartbeat_email_check
from json import load as jload


# Create basic logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# define file handler and set formatter
file_handler = logging.FileHandler('scidb_limited_access.log')
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
file_handler.setFormatter(formatter)

# add file handler to logger
logger.addHandler(file_handler)

logger.info('Program started')

# Setup authorization and connection details to the Volunteer response sheet
scopes = ['https://spreadsheets.google.com/feeds'
          ,'https://www.googleapis.com/auth/spreadsheets'
          ,'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scopes)
logger.debug('Authorizing Google credentials')
gc = gspread.authorize(creds)


# Connection details to Volunteers sheet
wb = gc.open('COVID-19 Pandemic: Scientist volunteer form (Responses)')
sheet = wb.worksheet('Volunteers')

# Download data and columns from sheet
volunteers  = sheet.get_all_values()
data = volunteers[1:]
columns = volunteers[0]

# cleanup trailing spaces
columns = [x.rstrip() for x in columns]

# Build dataframe and backup the volunteer data
volunteers_df = pd.DataFrame(data, columns=columns)
csv_filename = './Backups/All_Volunteers_Backup_'+str(datetime.now().date())+'.csv'
volunteers_df.to_csv(csv_filename, index=False)

volunteer_columns = {'Full name':'Name'
,'Email Address':'Email Address'
,'Phone number':'Phone Number'
,'Job Title (e.g. postdoc, graduate student, PI)':'Job Title'
,'Job Category 1':'Job Category 1'
,'Job Category 2':'Job Category 2'
,'Postal Code':'Postal Code'
,'County':'County'
,'City':'City'
,'State':'State'
,'Country':'Country'
,'Where do you currently do research (e.g. Harvard, Ohio State University, Novartis, etc.)':'Organization'
,'Do you have qPCR experience?':'qPCR Experience'
,'Do you have RNA extraction experience?':'RNA Extraction Experience'
,'Are authorized to do BSL2 work?':'BSL2 Certified'
,'Do you have experience working with RNA viruses?':'RNA Virus Experience'
,'Do you have RNA extraction kits you are willing to donate?':'Willing to donate RNA extraction kits'
,'Other equipment/reagents you are authorized and willing to donate (please separate with commas)?':'Other Donations'
,'Other skills/experience?':'Other Skills or Experience'
,'Anything else we should know about you?':'Further Notes'
,'Timestamp':'Joined Timestamp'
}
volunteers_df = volunteers_df[volunteer_columns.keys()]
volunteers_df = volunteers_df.rename(columns=volunteer_columns)
# Pad zeros to postal code
volunteers_df['Postal Code'] = volunteers_df['Postal Code'].apply(lambda x: x.zfill(5))

# Connection details to access request sheet
logger.debug('Connecting to Access Request Google sheet')
usr_wb = gc.open('FORM: DATA ACCESS REQUEST (Responses)')
usr_sheet = usr_wb.worksheet('Limited Database Access')

# Download data and columns from sheet
logger.debug('Downloading acess request data')
limited_users  = usr_sheet.get_all_values()
columns = limited_users[0]
data = limited_users[1:]

# Store and backup the limited user data
limited_users_df = pd.DataFrame(data, columns=columns)
csv_filename = './Backups/Limited_Users_Backup_'+str(datetime.now().date())+'.csv'
limited_users_df.to_csv(csv_filename, index=False)

# Load secrets
with open('local_secret.json') as f:
    secrets = jload(f)


# check how long since last email was sent
# need to send an email once a week to top Google blocking bot access
logger.debug('Running heartbeat email check')
heartbeat_email_check(secrets['GMAIL_PASS'])

logger.debug('Initiating access loop')
RunDTS = datetime.now()
Volunteers_Subset_TMP = pd.DataFrame()
Volunteers_Subset_DB = pd.read_csv('Volunteers_Subset_DB.txt', sep='\t')

for index, row in limited_users_df.iterrows():
    row['Run Notes'] = ''
    logger.debug('Working on %s - %s', str(row['Email To Name']),str(row['Unique ID']))
    
    # Check when the next run should be and update with this run's timestamp
    last_run_dts = min(parse_dts(row['Last Email Sent']), parse_dts(row['Last Run']))
    day_freq = int(row['Day Frequency'])
    next_run_dts = last_run_dts + timedelta(days=day_freq)
    next_run_dts_offset = next_run_dts - timedelta(hours=0, minutes=30) # Don't care about EXACT time
    row['Last Run'] = str(RunDTS)
    
    if RunDTS < next_run_dts_offset:
        logger.debug('Skipping check until %s',str(next_run_dts))
        row['Run Notes'] = 'Skipping check until ' + str(next_run_dts)
        continue
    
    filters_attempted = 0
    volunteer_ss = pd.DataFrame()
    
    # ZIP Code lookup
    if len(row['Zip Codes']) > 1:
        filters_attempted += 1
        zips = row['Zip Codes'].replace(',',';').split(';')
        zips = [x.strip() for x in zips] # remove leading spaces
        radii = row['Zip Radii'].replace(',',';').split(';')
        if len(radii)>0 and radii[0] != '':
            zip_list = get_zips(zips, radii)
        else:
            zip_list = get_zips(zips)
        volunteer_zip = volunteers_df.loc[volunteers_df['Postal Code'].isin(zip_list)]
        volunteer_ss = pd.concat([volunteer_ss,volunteer_zip])
        logger.debug('ZIP | Found %s Volunteers', str(len(volunteer_zip)))
        
    # City lookup
    if len(row['Cities']) > 1:
        if len(row['States']) > 1:
            filters_attempted += 1
            cities = row['Cities'].split(';')
            cities = [x.strip() for x in cities] # remove leading spaces
            states = row['States'].split(';')
            states = [x.strip() for x in states]
            volunteer_city = volunteers_df.loc[(volunteers_df['City'].isin(cities))&(volunteers_df['State'].isin(states))]
            volunteer_ss = pd.concat([volunteer_ss,volunteer_city])
            logger.debug('City | Found %s Volunteers',str(len(volunteer_city)))
        else:
            row['Run Notes'] = row['Run Notes'] + 'ERROR! Attempted city filter but missing state'
            logger.warning('City check is missing State')
    
    # County lookup
    if len(row['Counties']) > 1:
        if len(row['States']) > 1:
            filters_attempted += 1
            counties = row['Counties'].split(';')
            counties = [x.strip() for x in counties] # remove leading spaces
            states = row['States'].split(';')
            states = [x.strip() for x in states]
            volunteer_county = volunteers_df.loc[(volunteers_df['County'].isin(counties))&(volunteers_df['State'].isin(states))]
            volunteer_ss = pd.concat([volunteer_ss,volunteer_county])
            logger.debug('County | Found %s Volunteers',str(len(volunteer_county)))
        else:
            row['Run Notes'] = row['Run Notes'] + 'ERROR! Attempted county filter but missing state'
            logger.warning('County check is missing State')
    
    # State lookup
    if len(row['States']) > 1 and len(row['Cities']) <= 1 and len(row['Counties']) <= 1:
        filters_attempted += 1
        states = row['States'].split(';')
        states = [x.strip() for x in states]  # remove leading spaces
        volunteer_state = volunteers_df.loc[volunteers_df['State'].isin(states)]
        volunteer_ss = pd.concat([volunteer_ss,volunteer_state])
        logger.debug('State | Found %s Volunteers',str(len(volunteer_state)))
    
    # Drop duplicates and add plumbing columns
    volunteer_ss.drop_duplicates(subset='Email Address', keep=False, inplace=True)
    volunteer_ss['Unique ID'] = row['Unique ID']
    volunteer_ss['Updated'] = str(RunDTS)
    
    # Check if any filters were attemped, if not don't attempt to email or update the db
    volunteers_found = len(volunteer_ss)
    row['Volunteers Found'] = volunteers_found
    if filters_attempted == 0:
        row['Run Notes'] = row['Run Notes'] + 'ERROR! No filters attempted'
        logger.warning('No filters attempted')
        continue
    else:
        logger.debug('Attempted %s filter(s) and found %s unique volunteers',str(filters_attempted), str(volunteers_found))
    
    
    
    # Check if new volunteers have been added
    db_emails = set(Volunteers_Subset_DB['Email Address'].loc[(Volunteers_Subset_DB['Unique ID']==row['Unique ID'])])
    tmp_emails = set(volunteer_ss['Email Address'])
    email_diff = tmp_emails - db_emails
    if volunteers_found >0 and len(email_diff) >0:
        logger.debug('Found new volunteers, updating Subset DB')
        
        # drop previous values and add new results
        Volunteers_Subset_DB.drop(Volunteers_Subset_DB.index[Volunteers_Subset_DB['Unique ID']==row['Unique ID']], inplace=True)
        Volunteers_Subset_DB = pd.concat([Volunteers_Subset_DB, volunteer_ss], sort=False)
        if len(Volunteers_Subset_DB) >1:
            Volunteers_Subset_DB.to_csv('Volunteers_Subset_DB.txt', index=False, sep='\t')
        
        # prepare the subset for emailing
        volunteer_ss.drop('Unique ID',axis=1, inplace=True)
        logger.debug('Sending email')
        try:
            email_results(row['Email To Name'], row['Email Addresses'], volunteer_ss,  secrets['GMAIL_PASS'])
            row['Last Email Sent'] = str(RunDTS)
            row['Run Notes'] = row['Run Notes'] + str(len(email_diff)) + ' New volunteers found, sent email'
        except Exception as e:
            row['Run Notes'] = row['Run Notes'] + 'Error recieved when sending email'
            logger.exception('Error recieved while sending email')
    elif volunteers_found >0 and len(email_diff) == 0:
        row['Run Notes'] = row['Run Notes'] + 'No new volunteers found'

logger.debug('Preparing to update Google sheet with results')
# set Google sheet update range
cell_list = usr_sheet.range('K2:N'+str(len(limited_users_df)+1))

# grab output values for Google sheet
update_columns = ['Volunteers Found', 'Last Run', 'Last Email Sent', 'Run Notes']
dflist = limited_users_df[update_columns].values.tolist()

# load output into API friendly order and buld cell value data
cell_values = []
for row in dflist:
    for column in row:
        cell_values.append(column)
i=0
for cell in cell_list:
    cell.value = cell_values[i]
    i+=1
    
# update google sheet 'Limited Database Access'
if creds.access_token_expired:
    logger.debug('Google credentials expired, logging back in')
    creds.login()

logger.debug('Update Google sheet with results')
gresults = usr_sheet.update_cells(cell_list)
logger.info('Updated %s total cells in Google sheet',gresults['updatedCells'])

# make a copy of the DB just in case
logger.debug('Saving backup copy of the DB')
Volunteers_Subset_DB.to_csv('Volunteers_Subset_DB_BAK.txt', index=False, sep='\t')

# export PII data to AWS
pii_df = volunteers_df.copy()
pii_df.drop(index=pii_df[pii_df['Job Title'] == ''].index, inplace=True)
pii_df['url'] = 'https://covid19sci.org/'
logger.debug('Update PII data to AWS')
try:
    upload_aws(pii_df, 'SciDB_Volunteers_PII.csv', secrets['AWS_ACCESS_KEY'], secrets['AWS_SECRET_KEY'])
except:
    logger.debug("Error during PII Upload to AWS - Likely expired token, but not logging exception anymore")

# export non-PII data to AWS
pii_cols = ['Name','Email Address','Phone Number']
non_pii_df = volunteers_df.copy()
non_pii_df.drop(columns = pii_cols, inplace=True)
non_pii_df.drop(index=non_pii_df[non_pii_df['Job Title'] == ''].index, inplace=True)
non_pii_df['url'] = 'https://covid19sci.org/'
logger.debug('Update non-PII data to AWS')
try:
    upload_aws(non_pii_df, 'SciDB_Volunteers_No_PII.csv', secrets['AWS_ACCESS_KEY'], secrets['AWS_SECRET_KEY'])
except:
    logger.debug("Error during Non-PII Upload to AWS - Likely expired token, but not logging exception anymore")

logger.debug('Progam completed')
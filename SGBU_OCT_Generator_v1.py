# -*- coding: utf-8 -*-
'''
##################################
It's a real knife fight in Malibu!
##################################
'''

##################################
### - SQUAD OCT Generator    - ###
### - Author: Reid Landeen   - ###
### - Creation Date: 08Dec22 - ###
##################################

#
# 
#

######################################
### --- Import All the Things! --- ###
######################################

import pandas as pd
import numpy as np
import os
import fnmatch
import timeit
import sys
#import logging
#from tkinter import Tk, Label, Frame, Entry, Button, LabelFrame, Radiobutton, Text, Scrollbar, IntVar, Checkbutton, Menu, Variable, Toplevel
#from tkinter import filedialog as fd
#from threading import Thread
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)
pd.options.mode.chained_assignment = None
import logging
import logging.handlers
def logging_setup():
#--- logging module setup
    
    #logging.basicConfig(level=logging.INFO, filemode='a')
    #logger = logging.getLogger(__name__)
    client = 'SGBU'
    tool = 'OCT'
    log_time = pd.Timestamp('now').value
    log_file = f'.\log\{client}_{tool}_{log_time}.log'
    
    # Change root logger level from WARNING (default) to NOTSET in order for all messages to be delegated.
    logging.getLogger().setLevel(logging.NOTSET)
    
    # Add stdout handler, with level CRITICAL
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.CRITICAL)
    formater = logging.Formatter('%(name)-13s: %(levelname)-8s %(message)s')
    console.setFormatter(formater)
    logging.getLogger().addHandler(console)
    
    # Add file rotating handler, with level INFO
    rotatingHandler = logging.handlers.RotatingFileHandler(filename=log_file)
    rotatingHandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    rotatingHandler.setFormatter(formatter)
    logging.getLogger().addHandler(rotatingHandler)
    global logger
    logger = logging.getLogger(__name__)
    #/-- logging

global cwd
cwd = os.getcwd()

#######################
### --- /Import --- ###
#######################

###########################
### --- Definitions --- ###
###########################

def read_variables(cwd, path): 
    logger.info(f'Executing read_variables({cwd},{path})')
    print('Reading variables...')
    #global variables
    os.chdir(cwd)
    for file in os.listdir(os.getcwd()):
        if fnmatch.fnmatch(file, path):
            variables = pd.read_excel(file)
    
    return(variables)

def leads_input():
    leads_file = variables['Lead Input:'][0]
    logger.info(f'Readin in {leads_file}')
    print('Reading in {}'.format(leads_file))
    leads_in = pd.read_excel(leads_file)
    #leads_in.drop('Unnamed: 0', axis=1, inplace=True)
    leads_in['Action Timestamp (MST)'] = pd.to_datetime(leads_in['Action Timestamp (MST)'])
    #only use acitons that we want as defined in the variables file
    actions_keep = variables['Actions to Count as Leads'].dropna().tolist()
    leads_in = leads_in.loc[leads_in['Action Type'].isin(actions_keep)].reset_index()
    return(leads_in)

def gclid_cleaning(leads):
    logger.info('Executing gclid_cleaning(leads)')
    print('Cleaning GCLIDs...')
    # clean - get only Google entries
    leads = leads.loc[leads['Category'].str.contains('GAW', na=False)]
    leads.reset_index(drop=True, inplace=True)
    leads = leads[['Action Timestamp (MST)', 'landing_url', 'Profit']]
    
    # clean - get gclid from landing_url or referrer fields
    leads['gclid'] = leads['landing_url'].replace('.*&gclid=','', regex=True)
    leads.loc[leads['gclid'].str.contains('https://', na=False), 'gclid'] = np.nan
    
    leads = leads.loc[leads['gclid'].notnull()]
    leads.reset_index(drop=True, inplace=True)
    return(leads)

def output_cleaning(leads_in, output_cols):
    logger.info('Executing output_cleaning(leads_in, output_cols)')
    print('Setting up output...')
    leads_in.rename(columns={'Action Timestamp (MST)':'Conversion Time', 'gclid':'Google Click ID', 'Profit':'Conversion Value'}, inplace=True)
    leads_in['Conversion Name'] = variables['Conversion Name'][0]
    leads_in['Conversion Currency'] = 'USD'
    leads_out = leads_in.filter(output_cols)
    leads_out = leads_out.loc[leads_out['Conversion Value'].notnull()]
    leads_out.reset_index(drop=True, inplace=True)    

    #sort and drop dupe gclids
    leads_out.sort_values('Conversion Time', ascending=True, inplace=True)
    fudge_factor = variables['Fudge Factor'][0].astype(int)
    leads_out['Conversion Time'] = leads_out['Conversion Time']+pd.Timedelta(hours=fudge_factor)
    #leads_out.drop_duplicates(subset='Google Click ID', keep='first', inplace=True)
    leads_out.reset_index(drop=True, inplace=True)
    
    return(leads_out)

def write_out(leads_out):
    logger.info('Executing write_out(leads_out)')
    print('Writing output...')
    value_out = variables['Output Files:'][0]
    pd.DataFrame(columns=['Parameters:TimeZone=+0000']).to_csv(value_out, index=False)
    leads_out.to_csv(value_out, header=True, index=False, mode='a')    
    return

def google_upload(leads):
    logger.info('Executing google_upload(leads)')
    import gspread
    #from gspread_dataframe import set_with_dataframe
    from google.oauth2.service_account import Credentials
    from pydrive.auth import GoogleAuth
    from pydrive.drive import GoogleDrive
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    
    cred_path = variables['OCT Credentials'][0]
    credentials = Credentials.from_service_account_file(cred_path, scopes=scopes)
    
    gc = gspread.authorize(credentials)
    
    gauth = GoogleAuth()
    drive = GoogleDrive(gauth)
    # open a google sheet
    gs_key = variables['Google Sheet Key'][0]
    gs = gc.open_by_key(gs_key)
    # select a work sheet from its name
    worksheet1 = gs.worksheet('Sheet1')
    
    #clear worksheet
    worksheet1.clear()
    
    #sort leads latest to earliest; keep only n rows
    leads.sort_values('Conversion Time', ascending=False, inplace=True)
    size = variables['OCT Lookback'][0]
    leads = leads.loc[leads['Conversion Time']>=(leads['Conversion Time'].max() - pd.Timedelta(size,unit='day'))]
    leads.reset_index(drop=True, inplace=True) 
    leads['Conversion Time'] = leads['Conversion Time'].astype(str) #convert date to JSON friendly format
    #put in new worksheet
    print('Writing to Google Sheets...')
    worksheet1.update([pd.DataFrame(columns=['Parameters:TimeZone=+0000']).columns.values.tolist()] + [leads.columns.values.tolist()] + leads.values.tolist())

def new_output_cleaning(output_cols):
    logger.info('Executing new_output_cleaning(leads_in, output_cols)')
    print('Setting up output for new conversions...')
    
    leads_in_path = variables['Lead Input:'][0]
    if 'csv' in leads_in_path:
        leads_in = pd.read_csv(leads_in_path)
    else:
        leads_in = pd.read_excel(leads_in_path)
    
    lookback = variables['OCT Lookback'][0].astype(int)
    leads_in['Action Timestamp (MST)'] = pd.to_datetime(leads_in['Action Timestamp (MST)'])
    leads_in = leads_in.loc[leads_in['Action Timestamp (MST)']>=(leads_in['Action Timestamp (MST)'].max()-pd.Timedelta(lookback, unit='days'))]
    # clean - get gclid from landing_url or referrer fields
    leads_in['gclid'] = leads_in['landing_url'].replace('.*&gclid=','', regex=True)
    leads_in.loc[leads_in['gclid'].str.contains('https://', na=False), 'gclid'] = np.nan
    
    leads_in = leads_in.loc[leads_in['gclid'].notnull()]
    leads_in.reset_index(drop=True, inplace=True)
    leads_in.rename(columns={'Action Timestamp (MST)':'Conversion Time', 'gclid':'Google Click ID', 'Profit':'Conversion Value'}, inplace=True)
    
    leads_in['Conversion Name'] = np.nan
    leads_in.loc[(leads_in['New Customer']==1) & (leads_in['Action Type']=='Sale'), 'Conversion Name'] = 'OCT Profit - New Customer'
    leads_in.loc[(leads_in['New Customer'].isnull()) & (leads_in['Action Type']=='Sale'), 'Conversion Name'] = 'OCT Profit - Previous Purchaser'
    
    leads_in = leads_in.loc[leads_in['Conversion Name'].notnull()]
    leads_in['Conversion Value'] = (leads_in['Conversion Value'] * 0.05).where(leads_in['Conversion Name'] == 'OCT Profit - Previous Purchaser', leads_in['Conversion Value'])
    
    
    leads_in['Conversion Currency'] = 'USD'
    leads_out = leads_in.filter(output_cols)
    leads_out = leads_out.loc[leads_out['Conversion Value'].notnull()]
    leads_out.reset_index(drop=True, inplace=True)    

    #sort and drop dupe gclids
    leads_out.sort_values('Conversion Time', ascending=True, inplace=True)
    fudge_factor = variables['Fudge Factor'][0].astype(int)
    leads_out['Conversion Time'] = leads_out['Conversion Time']+pd.Timedelta(hours=fudge_factor)
    #leads_out.drop_duplicates(subset='Google Click ID', keep='first', inplace=True)
    leads_out.reset_index(drop=True, inplace=True)
    
    return(leads_out)

def new_google_upload(leads):
    logger.info('Executing google_upload(leads)')
    import gspread
    #from gspread_dataframe import set_with_dataframe
    from google.oauth2.service_account import Credentials
    #from pydrive.auth import GoogleAuth
    #from pydrive.drive import GoogleDrive
    scopes = ['https://www.googleapis.com/auth/spreadsheets',
              'https://www.googleapis.com/auth/drive']
    
    cred_path = variables['OCT Credentials'][1]
    credentials = Credentials.from_service_account_file(cred_path, scopes=scopes)
    
    gc = gspread.authorize(credentials)
    
    #gauth = GoogleAuth()
    #drive = GoogleDrive(gauth)
    # open a google sheet
    gs_key = variables['Google Sheet Key'][1]
    gs = gc.open_by_key(gs_key)
    # select a work sheet from its name
    worksheet1 = gs.worksheet('Sheet1')
    
    #clear worksheet
    worksheet1.clear()
    
    #sort leads latest to earliest; keep only n rows
    leads.sort_values('Conversion Time', ascending=False, inplace=True)
    size = variables['OCT Lookback'][0]
    leads = leads.loc[leads['Conversion Time']>=(leads['Conversion Time'].max() - pd.Timedelta(size,unit='day'))]
    leads.reset_index(drop=True, inplace=True) 
    leads['Conversion Time'] = leads['Conversion Time'].astype(str) #convert date to JSON friendly format
    #put in new worksheet
    print('Writing to Google Sheets...')
    worksheet1.update([pd.DataFrame(columns=['Parameters:TimeZone=+0000']).columns.values.tolist()] + [leads.columns.values.tolist()] + leads.values.tolist())


def main():
    logger.info('Executing main() method...')
    #--- logic
    # Read in leads
    # filter to past 90 days
    # clean gclid
    # Format date as mm/dd/yyyy hh:mm:ss
    # Conversion Value - calculate based on the lead_score in the daily leads output
    # Conversion Currency - USD
    # row header is Parameters:TimeZone=America/New_York
    # write output
    
    output_cols = ['Google Click ID', 
               'Conversion Name', 
               'Conversion Time', 
               #'Attributed Credit', 
               'Conversion Value', 
               'Conversion Currency']
    
    lookback = variables['OCT Lookback'][0].astype(int)
    
    leads_in = leads_input() # Read in leads
    leads_in = leads_in.loc[leads_in['Action Timestamp (MST)']>=(leads_in['Action Timestamp (MST)'].max()-pd.Timedelta(lookback, unit='days'))]
    leads_in = gclid_cleaning(leads_in) # clean gclids
    leads_out = output_cleaning(leads_in, output_cols) # prep output
    
    write_out(leads_out) # write output
    google_upload(leads_out) #uplaod to Google Sheets
    
    new_leads_out = new_output_cleaning(output_cols)
    new_google_upload(new_leads_out)
    new_out_path = variables['Output Files:'][1]
    print('Writing output to {}'.format(new_out_path))
    pd.DataFrame(columns=['Parameters:TimeZone=+0000']).to_csv(new_out_path, index=False)
    new_leads_out.to_csv(new_out_path, header=True, index=False, mode='a')    
    
    input("Press any key to end...")
    
############################
### --- /Definitions --- ###
############################

if __name__ == '__main__':
    try:
        logging_setup()
        logger.info('Running...')
        print('Running...')        
        global variables
        variables = read_variables(cwd, 'SGBU_OCT*.xlsx')
        #main()
    except:
        logger.exception('Got exception on handler')
        raise
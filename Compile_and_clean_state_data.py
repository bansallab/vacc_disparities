#!/usr/bin/env python
# coding: utf-8

# # This code takes separate files of demographically-based COVID-19 vaccination data and compiles it all into one dataset

# AM to-dos:
    # 1. Virginia's link is no longer working; Indiana & Pennsylvania only has the latest data. I'm mentioning these just in case you have a solution for them.
    # 2. Some states' data is still being accessed from the web. Could you please download those files and save them?

# SB to dos:
    # 1. ADD CODE FOR CALCULATING UNKNOWNRACE UNKNOWNETHNICITY BASED ON HISPANIC AS RACE
    # 2. LA, GA, WV : no hispanic disparity

import pandas as pd
import numpy as np
from datetime import datetime # date processing
import glob # for getting all filenames in a folder
import time
import matplotlib.font_manager
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['Open Sans']
matplotlib.rcParams['axes.unicode_minus'] = False

import warnings
warnings.filterwarnings('ignore') # setting ignore as a parameter

pd.options.mode.chained_assignment = None

start_time = time.time()

#drivelink='/Users/alexesmerritt/Library/CloudStorage/GoogleDrive-akm147@georgetown.edu/.shortcut-targets-by-id/1OujG6iNURSBo5seyBf5CjhOLdlVXuRYf/COVID_vaccination_dashboard'
drivelink = '../'


####################################
def combine_counties(df, ct1, ct2, state, partial_flag=False):
#combines ct2 into ct1

    #print(ct1, ct2, state)
    #print(df.loc[(df.COUNTY_NAME==ct1) & (df.STATE_NAME==state) & (df.CASE_TYPE=='Partial'),'CASES'])

    if partial_flag:
        a = df.loc[(df.COUNTY_NAME==ct1) & (df.STATE_NAME==state) & (df.CASE_TYPE=='Partial'),'CASES']
        b = df.loc[(df.COUNTY_NAME==ct2) & (df.STATE_NAME==state) & (df.CASE_TYPE=='Partial'), 'CASES']
        df.loc[(df.COUNTY_NAME==ct1) & (df.STATE_NAME==state) & (df.CASE_TYPE=='Partial'), 'CASES'] = int(a) + int(b)
    a = df.loc[(df.COUNTY_NAME==ct1) & (df.STATE_NAME==state) & (df.CASE_TYPE=='Complete'),'CASES']
    b = df.loc[(df.COUNTY_NAME==ct2) & (df.STATE_NAME==state) & (df.CASE_TYPE=='Complete'), 'CASES']
    df.loc[(df.COUNTY_NAME==ct1) & (df.STATE_NAME==state) & (df.CASE_TYPE=='Complete'), 'CASES'] = int(a) + int(b)
    
    return df


#################################
def clean_VA_demo(df_county_fips):
# hispanic as race
# provides partial/comp separately
# also have race with no county ('Not Reported', race with out of state (Out-of-State)
# provides fips
# unknown is correct

    print('Virginia: Have data over time, only keeping 2021-06-25') 
    
    df = pd.read_csv(drivelink+'/input_files/demo_vacc/VA_county_data.csv')

    # keep columns of relevance and rename
    df =df[['Report Date',"FIPS", "Vaccination Status", "Race and Ethnicity", "People by vaccination status count"]]
    df = df.rename(columns={'Report Date': 'DATE', "FIPS":'COUNTY', "Vaccination Status":'CASE_TYPE',"Race and Ethnicity": 'DEMO_GROUP', "People by vaccination status count":'CASES'})

    # clean data
    df = df[~df.COUNTY.isna()] # when FIPS is nan, data is for out of state or county is missing

    # fix case type categories
    df = df[df.CASE_TYPE != 'Not Reported']
    df.CASE_TYPE = df.CASE_TYPE.str.replace('Fully Vaccinated', 'Complete')
    df.CASE_TYPE = df.CASE_TYPE.str.replace('At Least One Dose', 'Partial')
      
    # format date and only keep latest data
    df['DATE'] = pd.to_datetime(df.DATE, format='%m/%d/%Y')

    # make long to wide for demo data
    demos=list(df['DEMO_GROUP'].unique())
    df2=pd.DataFrame(columns=['DATE','COUNTY','CASE_TYPE'])
    df2['DATE']=df['DATE']
    df2['COUNTY']=df['COUNTY']
    df2['CASE_TYPE']=df['CASE_TYPE']
    df2=df2.drop_duplicates()

    for demo in demos :
        df_demo=df.loc[df['DEMO_GROUP']==demo]
        df_demo=df_demo.rename(columns={'CASES':demo})
        df_demo=df_demo.drop('DEMO_GROUP', axis=1)
        df2=df2.merge(df_demo, how='outer', on=['DATE','COUNTY','CASE_TYPE'])


    df=df2

    df.columns.name = None # remove index name
    df = df.rename(columns={'Latino':'CASES_Hispanic', 'Asian or Pacific Islander':'CASES_Asian','Black':'CASES_Black',                            
                            'Two or more races':'CASES_OtherRace',                            
                            'Native American': 'CASES_Native',                            
                            'Not Reported':'CASES_UnknownRace', 
                            'White':'CASES_White'})
    df.CASES_OtherRace = df.CASES_OtherRace + df['Other Race']
    df = df.drop(columns=['Other Race'])
    df['CASES_NotHispanic'] = float('NaN')
    df['CASES_UnknownEthnicity'] = float('NaN')


    # add total column
    df['TOTAL'] = df['CASES_Hispanic'].fillna(0) + df['CASES_Asian'].fillna(0) + df['CASES_Black'].fillna(0) + df['CASES_OtherRace'].fillna(0) + df['CASES_Native'].fillna(0) + df['CASES_UnknownRace'].fillna(0) + df['CASES_White'].fillna(0)

    # only keep data for 6/25
    df = df[df.DATE == '2021-06-25']

    # fix county missing data
    #df.COUNTY = df.COUNTY.str.replace('Not Reported', 'Unknown')
    #df.COUNTY = df.COUNTY.str.replace('Out-of-State', 'Out of State')
    df = df[~df.COUNTY.isin(['Out-of-State', 'Not Reported'])]

    # add hispanic as race flag
    df['Hispanic_as_Race'] = 1
     
    # add state name
    df['STATE_NAME'] = 'Virginia'
    df['STATE'] = 51
    df['GEOFLAG'] = 'County'
    
    
    # add county name
    dfcf = df_county_fips[df_county_fips.STATE_NAME == 'Virginia'][['COUNTY', 'COUNTY_NAME']]
    df.COUNTY = df.COUNTY.astype(int)
    dfcf.COUNTY = dfcf.COUNTY.astype(int)
    df = df.merge(dfcf, on='COUNTY')
    
    return df

#################################
def clean_GA_demo(date):
# https://georgiadph.maps.arcgis.com/sharing/rest/content/items/e7378d64d3fa4bc2a67b2ea40e4748b0/data
# data is for all doses (partial or complete)-- unique people -- so calling it partial
# ethnicity separately
# realtime only

    print("Georgia: don't have over time but it might exist")

    filename = glob.glob(drivelink+'/input_files/demo_vacc/'+ date + '/' + 'Georgia_DPH*.xlsx')
                         
    # get race data
    df = pd.read_excel(filename[0], 'RACE_BY_COUNTY', engine = 'openpyxl')
    df =df[["COUNTY_ID", "COUNTY_NAME", 'RACE', 'PERSONVAX']]
    df = df.rename(columns={"COUNTY_ID":'COUNTY', "RACE": 'DEMO_GROUP', "PERSONVAX":'CASES'})
    df['CASE_TYPE'] = 'Partial' 
    #print('Georgia: the numbers are for both partial and complete combined; counting as partial')
                         
    # add in ethnicity data                     
    df2 = pd.read_excel(filename[0], 'ETHNICITY_BY_COUNTY', engine = 'openpyxl')
    df2 =df2[["COUNTYFIPS", "COUNTY_NAME", 'ETHNICTY', 'PERSONVAX']]
    df2 = df2.rename(columns={"COUNTYFIPS":'COUNTY', "ETHNICTY": 'DEMO_GROUP', "PERSONVAX":'CASES'})
    df2.loc[df2.DEMO_GROUP=='Unknown', 'DEMO_GROUP'] = 'CASES_UnknownEthnicity'
    df2['CASE_TYPE'] = 'Both'
    df = pd.concat([df,df2], ignore_index=True)
      
    # format date and only keep latest data
    df['DATE'] = pd.to_datetime(date+'-2021', format='%m-%d-%Y')
    #df['LATEST'] = 1
    
    # get rid of state entry
    df = df[df.COUNTY != 0]
    
    df.COUNTY_NAME = df.COUNTY_NAME.str.replace(' County', '')
    
    # make long to wide for demo data
    df = df.pivot(index=['DATE', 'COUNTY', 'COUNTY_NAME','CASE_TYPE'], columns='DEMO_GROUP', values='CASES').reset_index()
    df.columns.name = None # remove index name
    df = df.rename(columns={'Hispanic':'CASES_Hispanic',''                            'Asian':'CASES_Asian',                            'Black':'CASES_Black',                            'Other':'CASES_OtherRace',                            'American Indian or Alaska Native': 'CASES_Native',                            'Unknown':'CASES_UnknownRace', 'White':'CASES_White'})
    df['CASES_UnknownEthnicity'] = float('NaN')

    
    # add total column
    df['TOTAL'] = df['CASES_Hispanic'].fillna(0) + df['CASES_Asian'].fillna(0) + df['CASES_Black'].fillna(0) + df['CASES_OtherRace'].fillna(0) + df['CASES_Native'].fillna(0) + df['CASES_UnknownRace'].fillna(0) + df['CASES_White'].fillna(0)
    df['CASES_NotHispanic'] = df['TOTAL']-df['CASES_UnknownEthnicity']-df['CASES_Hispanic']
    
    # add hispanic as race flag
    df['Hispanic_as_Race'] = 0
     
    # add state name
    df['STATE_NAME'] = 'Georgia'

    # add what scale the data is on
    df['GEOFLAG'] = 'County'
    
    return df
                         
################################
def clean_LA_demo(date):
#https://ladhh.maps.arcgis.com/apps/webappviewer/index.html?id=3b9b6f22d92f4d688f1c21e9d154cae2
# provides both partial, complete separately
# ethnicity separately
# realtime only


    print('Louisiana: have over time, only keeping 6/25/21')

    filename = glob.glob(drivelink+'/input_files/demo_vacc/'+ date + '/' + 'Vaccination Demographic Counts (State, Region, Parish).csv')
                         
    # get race data
    df = pd.read_csv(filename[0])
    df =df[["Geography","Geographic Level","FIPS Code","Demographic Group","Vaccine Status","Count"]]
    df = df[df['Geographic Level'] == 'PARISH']
    df = df[df['Vaccine Status'] != 'Unvaccinated']
    df = df[df['Demographic Group'].isin(['Black', 'Other Race', 'Unknown Race', 'White'])]
    df =df[["Geography","FIPS Code","Demographic Group","Vaccine Status","Count"]]
    df = df.rename(columns={"FIPS Code":'COUNTY', "Geography": "COUNTY_NAME", 'Demographic Group': 'DEMO_GROUP',"Vaccine Status":"CASE_TYPE","Count":'CASES'})
      
    # format date and only keep latest data
    df['DATE'] = pd.to_datetime(date+'-2021', format='%m-%d-%Y')
    # add latest
    #df['LATEST'] = 1
        
    # partial data is actually partial only so need to combine
    a = df.loc[df.CASE_TYPE=='Complete', 'CASES'].fillna(0)
    b = df.loc[df.CASE_TYPE=='Incomplete', 'CASES'].fillna(0)
    total = [xx + yy for (xx,yy) in zip(a,b)]
    df.loc[df.CASE_TYPE=='Incomplete', 'CASES'] = total
    df.CASE_TYPE = df.CASE_TYPE.replace("Incomplete", "Partial")

    df.COUNTY_NAME = df.COUNTY_NAME + ' Parish'
    
    # make long to wide for demo data
    df = df.pivot(index=['DATE', 'COUNTY', 'COUNTY_NAME','CASE_TYPE'], columns='DEMO_GROUP', values='CASES').reset_index()
    df.columns.name = None # remove index name
    df = df.rename(columns={'Black':'CASES_Black',                            
                            'Other Race':'CASES_OtherRace',                            
                            'Unknown Race':'CASES_UnknownRace', 
                            'White':'CASES_White'})
    df['CASES_UnknownEthnicity'] = float('NaN')
    df['CASES_Hispanic'] = float('NaN')
    df['CASES_NotHispanic'] = float('NaN')
    df['CASES_Native'] = float('NaN')
    df['CASES_Asian'] = float('NaN')
    df['CASES_UnknownEthnicity'] = float('NaN')
    
    # add total column
    df['TOTAL'] = df['CASES_Black'] + df['CASES_OtherRace'] + df['CASES_UnknownRace'] + df['CASES_White']
    
    # add hispanic as race flag
    df['Hispanic_as_Race'] = float('NaN')
     
    # add state name
    df['STATE_NAME'] = 'Louisiana'

    # add what scale the data is on
    df['GEOFLAG'] = 'County'
    
    return df

################################
def clean_MO_demo(date):
#https://health.mo.gov/living/healthcondiseases/communicable/novel-coronavirus/data/data-download-vaccine.php
# data is for separately partial, comp
# ethnicity separately
# all dates starting Jan 9

    print('Missouri: have data through 6/25/21, only keeping 6/25/21')

    df = pd.DataFrame()
   
    # get completed ethnicity data
    filename = glob.glob(drivelink+'/input_files/demo_vacc/'+ date + '/'+'Completed_Vaccinations_by_Ethnicity_data.csv')
    dfx = pd.read_csv(filename[0], encoding="UTF-16 LE", sep='\t', skiprows=1, header=None, names =['DATE', 'COUNTY_NAME', 'DEMO_GROUP', 'CASES'])
    dfx['CASE_TYPE'] = 'Complete'
    dfx.DEMO_GROUP = dfx.DEMO_GROUP.replace('Unknown', 'CASES_UnknownEthnicity')
    df_comp = dfx.copy()
    df = pd.concat([df, dfx], ignore_index=True)

    # get partial ethnicity data
    filename = glob.glob(drivelink+'/input_files/demo_vacc/'+ date + '/' + 'Initiated_Vaccinations_by_Ethnicity_data*.csv')
    dfx = pd.read_csv(filename[0], encoding="UTF-16 LE", sep='\t', skiprows=1, header=None, names =['DATE', 'COUNTY_NAME', 'DEMO_GROUP', 'CASES'])
    dfx['CASE_TYPE'] = 'Partial'
    dfx.DEMO_GROUP = dfx.DEMO_GROUP.replace('Unknown', 'CASES_UnknownEthnicity')
    df_part = dfx.copy()
    dfx = pd.concat([df_part,df_comp ], ignore_index=True)
    dfx = dfx.groupby(['DATE', 'COUNTY_NAME', 'DEMO_GROUP'])['CASES'].sum().reset_index() # partial = partial + complete
    dfx['CASE_TYPE'] = 'Partial'  
    df = pd.concat([df, dfx], ignore_index=True)

    # get completed race data
    filename = glob.glob(drivelink+'/input_files/demo_vacc/'+ date + '/' + 'Completed_Vaccinations_by_Race_data*.csv')
    dfx = pd.read_csv(filename[0], encoding="UTF-16 LE", sep='\t', skiprows=1, header=None, names =['DATE', 'COUNTY_NAME', 'DEMO_GROUP', 'CASES'])
    dfx['CASE_TYPE'] = 'Complete'
    df_comp= dfx.copy()
    df = pd.concat([df, dfx], ignore_index=True)

    # get patial race data
    filename = glob.glob(drivelink+'/input_files/demo_vacc/'+ date + '/' + 'Initiated_Vaccinations_by_Race_data*.csv')
    dfx = pd.read_csv(filename[0], encoding="UTF-16 LE", sep='\t', skiprows=1, header=None, names =['DATE', 'COUNTY_NAME', 'DEMO_GROUP', 'CASES'])
    dfx['CASE_TYPE'] = 'Partial'
    df_part = dfx.copy()
    dfx = pd.concat([df_part,df_comp ], ignore_index=True)
    dfx = dfx.groupby(['DATE', 'COUNTY_NAME', 'DEMO_GROUP'])['CASES'].sum().reset_index() # partial = partial + complete
    dfx['CASE_TYPE'] = 'Partial'  
    df = pd.concat([df, dfx], ignore_index=True)

    # need to make data cumulative
    df['DATE'] = pd.to_datetime(df.DATE, format='%m/%d/%Y').dt.date
    dmax = df.DATE.max()
    df = df.groupby(['COUNTY_NAME', 'DEMO_GROUP', 'CASE_TYPE'])['CASES'].sum().reset_index()
       
    # format date and only keep latest data
    #df['DATE'] = pd.to_datetime(df.DATE, format='%m/%d/%Y').dt.date
    #df.loc[df.DATE==df.DATE.max(), 'LATEST'] = 1
    df['DATE'] = dmax
    #df['DATE'] = pd.to_datetime(df.DATE, format='%m/%d/%Y').dt.date
    df['LATEST'] = 1
             
    # add state name
    df['STATE_NAME'] = 'Missouri'
    df['STATE'] = 29

    # combine some cities
    #df = combine_counties(df, 'Jasper', 'Joplin', 'Missouri', True)
    #df = combine_counties(df, 'Jackson', 'Kansas City', 'Missouri', True)
    df.COUNTY_NAME = df.COUNTY_NAME.replace('Joplin', 'Jasper')
    df.COUNTY_NAME = df.COUNTY_NAME.replace('Kansas City', 'Jackson')
    df.COUNTY_NAME = df.COUNTY_NAME.replace('St. Louis City', 'St. Louis city')

    df = df.groupby(['STATE_NAME','COUNTY_NAME', 'DEMO_GROUP', 'CASE_TYPE', 'DATE'])['CASES'].sum().reset_index()

    df.COUNTY_NAME = df.COUNTY_NAME.astype(str)
    df = df[~df.COUNTY_NAME.isin(["Out-of-State", "Unknown Jurisdiction", "Unknown State"])]

    # make long to wide for demo data
    df = df.pivot(index=['STATE_NAME','DATE', 'COUNTY_NAME','CASE_TYPE'], columns='DEMO_GROUP', values='CASES').reset_index()
    df.columns.name = None # remove index name
    df['Other Race'] = df['Other Race'] + df['Multi-racial']
    df['Asian'] = df['Asian'] + df['Native Hawaiian or Other Pacif']
    df = df.drop(columns=['Multi-racial', 'Native Hawaiian or Other Pacif'])
    df = df.rename(columns={'Black or African-American':'CASES_Black','Asian': 'CASES_Asian','Hispanic or Latino':'CASES_Hispanic',                            'Other Race':'CASES_OtherRace','American Indian or Alaska Nati': 'CASES_Native',                            'Unknown':'CASES_UnknownRace', 'White':'CASES_White', 'Not Hispanic or Latino':"CASES_NotHispanic"})   
    
    # add total column
    df['TOTAL'] = df['CASES_Hispanic'].fillna(0) + df['CASES_Asian'].fillna(0) + df['CASES_Black'].fillna(0) + df['CASES_OtherRace'].fillna(0) + df['CASES_Native'].fillna(0) + df['CASES_UnknownRace'].fillna(0) + df['CASES_White'].fillna(0)

    # add hispanic as race flag
    df['Hispanic_as_Race'] = 0


    # add what scale the data is on
    df['GEOFLAG'] = 'County'

    df = df[~df.COUNTY_NAME.isin(["Out-of-State", "Unknown Jurisdiction", "Unknown State"])]
    
    return df

################################
def clean_WV_demo(date):
# screenshot table here: https://dhhr.wv.gov/COVID-19/Pages/default.aspx (Jefferson, Pendleton, Wyoming)
# extract using: https://extracttable.com
# no ethnicity

    print('West Virginia: only have 6/25/21')

    files = glob.glob(drivelink+'/input_files/demo_vacc/'+ date + '/' + 'Screen*.csv')
    
    df = pd.DataFrame()
    for filename in files:
        dfx = pd.read_csv(filename, skiprows = 1, header=None, names=['COUNTY_NAME', 'CASES_Black', 'x1', 'CASES_OtherRace', 'x2','CASES_UnknownRace', 'x3','CASES_White', 'x4'], thousands=',')
        dfx =dfx[['COUNTY_NAME', 'CASES_Black', 'CASES_OtherRace','CASES_UnknownRace','CASES_White']]
        df = pd.concat([df,dfx], ignore_index=True)

    # format date and only keep latest data
    df['DATE'] = pd.to_datetime(date+'-2021', format='%m-%d-%Y')
    df['CASE_TYPE']= 'Partial'
        
    # format case columns as integers
    df = df.drop(df[df.CASES_Black == 'Count'].index)
    df = df.replace(',','', regex=True) # remove commas from numbers
    df['CASES_Black'] = df['CASES_Black'].astype(int)
    df['CASES_OtherRace'] = df['CASES_OtherRace'].astype(int)
    df['CASES_UnknownRace'] = df['CASES_UnknownRace'].astype(int)
    df['CASES_White'] = df['CASES_White'].astype(int)

    # add latest
    df['LATEST'] = 1
    
    df['CASES_UnknownEthnicity'] = float('NaN')
    df['CASES_Hispanic'] = float('NaN')
    df['CASES_NotHispanic'] = float('NaN')
    df['CASES_Native'] = float('NaN')
    df['CASES_Asian'] = float('NaN')
    df['CASES_UnknownEthnicity'] = float('NaN')
    
    # add total column
    df['TOTAL'] = df['CASES_Hispanic'].fillna(0) + df['CASES_Asian'].fillna(0) + df['CASES_Black'].fillna(0) + df['CASES_OtherRace'].fillna(0) + df['CASES_Native'].fillna(0) + df['CASES_UnknownRace'].fillna(0) + df['CASES_White'].fillna(0)
    
    # add hispanic as race flag
    df['Hispanic_as_Race'] = float('NaN')
     
    # add state name
    df['STATE_NAME'] = 'West Virginia'

    # add what scale the data is on
    df['GEOFLAG'] = 'County'
    
    df = df[~df.COUNTY_NAME.isin(["County Name"])]

    
    return df 
    

#################################
def clean_TX_demo():
# hispanic as race
# also has county = "Other"
# unknown is correct
# getting both partial, compelte separtely

    print('Texas: only have 7/01/2021')

    
    file_path=drivelink+'/input_files/Texas 2021-07-01.xlsx'
    # get data
    df = pd.read_excel(file_path, 'By County, Race', engine = 'openpyxl')
    
    # make wide to long for CASE_TYPE
    df = df.rename(columns={'County Name ':'COUNTY_NAME', 'Race/Ethnicity': 'DEMO_GROUP', "People Vaccinated with at least One Dose":'Partial',"People Fully Vaccinated ":'Complete'})
    df = df.drop(columns= ['Doses Administered'])
    df = pd.melt(df, id_vars=['COUNTY_NAME', 'DEMO_GROUP'], value_vars=["Partial", 'Complete'],             var_name='CASE_TYPE', value_name='CASES')

        
    # make long to wide for demo data
    df = df.pivot(index=['COUNTY_NAME', 'CASE_TYPE'], columns='DEMO_GROUP', values='CASES').reset_index()
    df.columns.name = None # remove index name
    df = df.drop(columns=['Total'])
    df = df.rename(columns={'Hispanic':'CASES_Hispanic','Asian':'CASES_Asian', 'Black':'CASES_Black',                            'Other':'CASES_OtherRace',                            'Unknown':'CASES_UnknownRace', 'White':'CASES_White'})
    df['CASES_Native'] = float('NaN')
    df['CASES_NotHispanic'] = float('NaN')
    df['CASES_UnknownEthnicity'] = float('NaN')
    # add total column
    df['TOTAL'] = df['CASES_Hispanic'].fillna(0) + df['CASES_Asian'].fillna(0) + df['CASES_Black'].fillna(0) + df['CASES_OtherRace'].fillna(0) + df['CASES_Native'].fillna(0) + df['CASES_UnknownRace'].fillna(0) + df['CASES_White'].fillna(0)
        
    # add state name
    df['STATE_NAME'] = 'Texas'
    
    # add date
    df['DATE'] = pd.read_excel(file_path, 'About the Data', skiprows=1, engine = 'openpyxl').columns[1]
    
    # drop missing county names
    df = df[~df.COUNTY_NAME.isin(['Other', 'Unknown', 'Grand Total', 'nan', float('NaN')])]
    
    # add latest
    df['LATEST'] = 0
    
    # add hispanic as race flag
    df['Hispanic_as_Race'] = 1

    # add what scale the data is on
    df['GEOFLAG'] = 'County'
    
    return df

    
#################################
def clean_TN_demo():
# hispanic not as race
# is cumulative
# race with out of state (OUT OF STATE)
# unknown is correct
# First dose ony
# Has multiple dates

    print('Tennesse: have over time, only keeping 6/23/21')

    
    df = pd.read_excel(drivelink+'/input_files/demo_vacc/TN_county_data.XLSX', engine = 'openpyxl')

    # fix unknown ethnicity label
    df.loc[(df.CATEGORY=='ETHN') & (df.CAT_DETAIL=='UNKNOWN'), 'CAT_DETAIL'] = 'ETHN_UNKNOWN'

    # keep columns of relevance and rename
    df = df[df.CATEGORY != 'SEX']
    df = df[['DATE', 'COUNTY', 'CAT_DETAIL', 'RECIPIENT_COUNT']]
    df = df.rename(columns={"COUNTY":'COUNTY_NAME', 'CAT_DETAIL': 'DEMO_GROUP', "RECIPIENT_COUNT":'CASES'})

    # cleanup county name
    df.COUNTY_NAME = df.COUNTY_NAME.str.title()

    df.COUNTY_NAME = df.COUNTY_NAME.replace('Dekalb', 'DeKalb')
    df.COUNTY_NAME = df.COUNTY_NAME.replace('Mcminn', 'McMinn')
    df.COUNTY_NAME = df.COUNTY_NAME.replace('Mcnairy', 'McNairy')

    df = df[~df.COUNTY_NAME.isin(['Out Of State'])]


    #removing duplicate 
    df['dup']=df.duplicated(subset=['DATE', 'COUNTY_NAME', 'DEMO_GROUP'],keep='first')
    df=df.loc[df['dup']==False]
    df = df.drop(columns=['dup'])
    # make long to wide for demo data
    df = df.pivot(values='CASES',index=['DATE','COUNTY_NAME'], columns='DEMO_GROUP').reset_index()
    df.columns.name = None # remove index name
    df = df.rename(columns={'HISPANIC OR LATINO':'CASES_Hispanic',                            'NOT HISPANIC OR LATINO':'CASES_NotHispanic',                            'ASIAN':'CASES_Asian',                            'BLACK OR AFRICAN AMERICAN':'CASES_Black',                            'OTHER/MULTIRACIAL': 'CASES_OtherRace',                            'UNKNOWN':'CASES_UnknownRace', 'WHITE':'CASES_White',                             'ETHN_UNKNOWN': 'CASES_UnknownEthnicity'})
    df['CASES_Native'] = float('NaN')


    # add total column
    df['TOTAL'] = df['CASES_Hispanic'].fillna(0) + df['CASES_Asian'].fillna(0) + df['CASES_Black'].fillna(0) + df['CASES_OtherRace'].fillna(0) + df['CASES_Native'].fillna(0) + df['CASES_UnknownRace'].fillna(0) + df['CASES_White'].fillna(0)



    # add hispanic as race flag
    df['Hispanic_as_Race'] = 1
     
    # add state name
    df['STATE_NAME'] = 'Tennessee'


    # fix county missing data
    df.COUNTY_NAME = df.COUNTY_NAME.str.replace('Other', 'Unknown')

    # label latest data
    df.loc[df.DATE == df.DATE.max(), 'LATEST'] = 1
    df.loc[df.DATE != df.DATE.max(), 'LATEST'] = 0
    df = df[df.DATE == '2021-06-23 00:00:00']

    df['Hispanic_as_Race'] = 0

    # add case type
    df['CASE_TYPE'] = 'Partial'

    # add complete data
    df2 = df.copy()
    df2['CASE_TYPE'] = df2.CASE_TYPE.str.replace('Partial', 'Complete')
    df2['TOTAL'] = float('NaN')
    df2['CASES_Hispanic']  = float('NaN')
    df2['CASES_NotHispanic']  = float('NaN')
    df2['CASES_UnknownEthnicity']  = float('NaN')
    df2['CASES_Asian'] = float('NaN')
    df2['CASES_Black']  = float('NaN')
    df2['CASES_OtherRace']  = float('NaN')
    df2['CASES_Native']  = float('NaN')
    df2['CASES_UnknownRace']  = float('NaN')
    df2['CASES_White'] =  float('NaN')
    df = pd.concat([df, df2], ignore_index=True)

    # fix county missing data
    #df.COUNTY = df.COUNTY.str.replace('Out of State', 'Out of State')

    # add what scale the data is on
    df['GEOFLAG'] = 'County'
    
    return df

#################################
def clean_PA_demo():
# hispanic not as race
# is cumulative
# race with out of state (OUT-OF-STATE)
# unknown is incorrect
# Both doses (partial is 1 only, fixed it)

    print('Pennsylvania')

    
    df1 = pd.read_csv('https://data.pa.gov/api/views/x5z9-57ub/rows.csv') #race
    df1 = df1.rename(columns = {'Partially Covered  Unknown': 'Partially Covered Unknown Race', 'Fully Covered Unknown': 'Fully Covered Unknown Race'})
    df2 = pd.read_csv('https://data.pa.gov/api/views/7ruj-m7k6/rows.csv') #ethnicity
    df2 = df2.rename(columns = {'Partially Covered Unknown': 'Partially Covered Unknown Ethnicity', 'Fully Covered Unknown': 'Fully Covered Unknown Ethnicity'})
    df3 = pd.read_csv('https://data.pa.gov/api/views/gcnb-epac/rows.csv') #Total
    df3 = df3.rename(columns = {'Partially Vaccinated': 'Partially Covered TOTAL', 'Fully Vaccinated': 'Fully Covered TOTAL'})
    df3 = df3.drop(columns = ['County Population', 'Rate Partially Vaccinated per 100,000','Rate Fully Vaccinated per 100,000'])
    
    # clean up datasets
    df1 = df1.rename(columns={"County Name":'COUNTY_NAME'})
    df1.COUNTY_NAME = df1.COUNTY_NAME.str.title()
    df2 = df2.rename(columns={"County Name":'COUNTY_NAME'})
    df2.COUNTY_NAME = df2.COUNTY_NAME.str.title()
    df3 = df3.rename(columns={"County Name":'COUNTY_NAME'})
    df3.COUNTY_NAME = df3.COUNTY_NAME.str.title()
    
    # combine datasets
    df = df1.merge(df2, on='COUNTY_NAME')
    df = df.merge(df3, on='COUNTY_NAME')


    # make partial = partial + complete
    colnames = list(df.columns)
    for col in colnames:
        if col.startswith('Partially'):
            demo = col.split('Covered ')[1]
            df['Partially Covered '+demo]  = df['Partially Covered '+demo] + df['Fully Covered '+demo]
    
    
    # make wide to long for CASE_TYPE
    col_names = list(df.columns)
    col_names.remove('COUNTY_NAME')
    df = pd.melt(df, id_vars=['COUNTY_NAME'], value_vars=col_names,             var_name='CASE_TYPE', value_name='CASES')
    df['DEMO_GROUP'] = df.CASE_TYPE.str.split('Covered ').str[1]
    df['CASE_TYPE'] = df.CASE_TYPE.str.split(' Covered').str[0]
    df.CASE_TYPE = df.CASE_TYPE.str.replace('Partially', 'Partial').str.replace('Fully', 'Complete') 
        
    # make long to wide for demo data
    df = df.pivot(index=['COUNTY_NAME', 'CASE_TYPE'], columns='DEMO_GROUP', values='CASES').reset_index()
    df.columns.name = None # remove index name
    df = df.rename(columns={'Hispanic':'CASES_Hispanic','Not Hispanic':'CASES_NotHispanic','Asian':'CASES_Asian','African American':'CASES_Black', 'Native American': 'CASES_Native','Multiple Other': 'CASES_OtherRace', 'Unknown Race':'CASES_UnknownRace',       'White':'CASES_White','Unknown Ethnicity': 'CASES_UnknownEthnicity'})
    df.CASES_Asian = df.CASES_Asian + df['Pacific Islander']
    df = df.drop(columns=['Pacific Islander'])
    
    # fix unknown
    df.CASES_UnknownRace = df.TOTAL - df['CASES_Asian'].fillna(0) - df['CASES_Black'] - df['CASES_OtherRace']-                           df['CASES_White'] - df['CASES_Native'].fillna(0)
    df.CASES_UnknownEthnicity = df.TOTAL - df['CASES_Hispanic'].fillna(0) - df['CASES_NotHispanic'].fillna(0)
       
    # add hispanic as race flag
    df['Hispanic_as_Race'] = 0
     
    # add state name
    df['STATE_NAME'] = 'Pennsylvania'
    
    # add latest flag
    # label latest data
    df['DATE'] = datetime.strftime(datetime.now(), '%Y-%m-%d')
    df['LATEST'] = 1
    
    df['Hispanic_as_Race'] = 0
       
    # fix county missing data
    df.COUNTY_NAME = df.COUNTY_NAME.str.replace('Out-Of-State', 'Out of State')

    # add what scale the data is on
    df['GEOFLAG'] = 'County'
    
    df = df[~df.COUNTY_NAME.isin(["Out of State"])]
    df.COUNTY_NAME = df.COUNTY_NAME.replace('Mckean','McKean')

    
    return df

################################
def NC_race(dft):
    dft = dft.rename(columns={"County ": "COUNTY_NAME", "Week of": "DATE",
                              'American Indian or Alaskan Native': 'CASES_Native', 
                              'Asian or Pacific Islander':'CASES_Asian', 
                              'Black or African American': 'CASES_Black', 
                              'White': 'CASES_White', 
                              'Other': 'CASES_OtherRace',
                              'Suppressed': 'CASES_UnknownRace'})
    dft['CASES_UnknownRace'] = dft['CASES_UnknownRace'] + dft['Missing or Undisclosed']
    dft = dft.drop(columns=['Index', 'Missing or Undisclosed'])
    dft.DATE = pd.to_datetime(dft.DATE, format='%m/%d/%Y')
    
    return dft

################################
def NC_ethnicity(dft):
    dft = dft.rename(columns={"County ": "COUNTY_NAME", "Week of": "DATE",                               'Hispanic': 'CASES_Hispanic',                               'Non-Hispanic':'CASES_NotHispanic',                               'Suppressed': 'CASES_UnknownEthnicity'})
    dft['CASES_UnknownEthnicity'] = dft['CASES_UnknownEthnicity'] + dft['Missing or Undisclosed']
    dft = dft.drop(columns=['Index', 'Missing or Undisclosed'])
    dft.DATE = pd.to_datetime(dft.DATE, format='%m/%d/%Y')
    
    return dft

#################################
def clean_NC_demo():
# hispanic  not as race
# unknown is correct
# 1 dose only
# not cumulative
# download from: https://covid19.ncdhhs.gov/dashboard/data-behind-dashboards (has history)
# has all dates

    print("North Carolina: have over time, only keeping 6/28/21")
    
    path = drivelink+'/input_files/demo_vacc/NC/'
    df1 = pd.read_excel(path+'Vaccination_Race-NC-Cnty.xlsx', engine = 'openpyxl') # race, partial
    df2 = pd.read_excel(path+'Vaccination_Race-NC-Cnty-2.xlsx', engine = 'openpyxl') # race, complete
    df3 = pd.read_excel(path+'Vaccination_Ethnicity-NC-Cnty.xlsx', engine = 'openpyxl') # ethnicity, partial
    df4 = pd.read_excel(path+'Vaccination_Ethnicity-NC-Cnty-2.xlsx', engine = 'openpyxl') # eth, complet
    df5 = pd.read_excel(path+'Vaccination_Race-Fed-Cnty.xlsx', engine = 'openpyxl') # fed race, partial
    df6 = pd.read_excel(path+'Vaccination_Race-Fed-Cnty-2.xlsx', engine = 'openpyxl') # fed race, complete
    df7 = pd.read_excel(path+'Vaccination_Ethnicity-Fed-Cnty.xlsx', engine = 'openpyxl') # fed eth, partial
    df8 = pd.read_excel(path+'Vaccination_Ethnicity-Fed-Cnty-2.xlsx', engine = 'openpyxl') # fed eth, complete
    
    df = pd.DataFrame()

    dft = NC_race(df1)
    dft['CASE_TYPE'] = 'Partial'
    df = pd.concat([df, dft], ignore_index=True, axis=0)
    dft = NC_race(df2)
    dft['CASE_TYPE'] = 'Complete'
    df = pd.concat([df, dft], ignore_index=True, axis=0)
    dft = NC_race(df5)
    dft['CASE_TYPE'] = 'Partial'
    df = pd.concat([df, dft], ignore_index=True, axis=0)
    dft = NC_race(df6)
    dft['CASE_TYPE'] = 'Complete'
    df = pd.concat([df, dft], ignore_index=True, axis=0)
    dft = NC_ethnicity(df3)
    dft['CASE_TYPE'] = 'Partial'
    df = pd.concat([df, dft], ignore_index=True, axis=0)
    dft = NC_ethnicity(df4)
    dft['CASE_TYPE'] = 'Complete'
    df = pd.concat([df, dft], ignore_index=True, axis=0)
    dft = NC_ethnicity(df7)
    dft['CASE_TYPE'] = 'Partial'
    df = pd.concat([df, dft], ignore_index=True, axis=0)
    dft = NC_ethnicity(df8)
    dft['CASE_TYPE'] = 'Complete'
    df = pd.concat([df, dft], ignore_index=True, axis=0)    
    df = df.fillna(0)

    # need to make data cumulative
    df['DATE'] = pd.to_datetime(df.DATE)
    dmax = '2021-06-28'
    df = df[df.DATE <= datetime.strptime(dmax, "%Y-%m-%d")]
    df = df.groupby(['COUNTY_NAME', 'CASE_TYPE'])[['CASES_White', 'CASES_Black', 'CASES_Asian', 'CASES_Native', 'CASES_OtherRace', 'CASES_UnknownRace', 'CASES_Hispanic', 'CASES_NotHispanic', 'CASES_UnknownEthnicity']].sum().reset_index()

    # add latest flag
    df['DATE'] = dmax
    df['DATE'] = pd.to_datetime(df.DATE)
    df['LATEST'] = 1
    
    df['Hispanic_as_Race'] = 0
    df['STATE_NAME'] = "North Carolina"
       
    # fix county missing data
    #df.COUNTY_NAME = df.COUNTY_NAME.str.replace('Out-Of-State', 'Out of State')
    df = df[df.COUNTY_NAME != 'Out of State']

    # add what scale the data is on
    df['GEOFLAG'] = 'County'
    
    df = df[~df.COUNTY_NAME.isin(['Missing'])]
    
    return df

####################################
def add_FIPS(df):
    
    # load countyname to fips code
    df_county_fips = pd.read_csv(drivelink+"/other_data/countyname_fips.csv")

    # clean up countyname to fips code
    df_county_fips['COUNTY'] = df_county_fips["Statefips"].astype(int)*1000 + df_county_fips["Countyfips"].astype(int)
    df_county_fips['COUNTY_NAME'] = df_county_fips['County']
    df_county_fips['STATE'] = df_county_fips["Statefips"]

    # add in state names
    df_state = pd.read_csv(drivelink+"/other_data/state_fips_abbrev.txt", sep="\t", header=None, names=['state_name', 'statefips', 'State'])
    df_county_fips = df_county_fips.merge(df_state, on='State', how='inner')
    df_county_fips['STATE_NAME'] = df_county_fips['state_name']
        
    # cleanup
    df_county_fips = df_county_fips[["COUNTY", "STATE", "COUNTY_NAME", "STATE_NAME"]]
    
    # split vacc data into county level, state level
    dfcounty = df[df.GEOFLAG=='County']
    #dfstate = df[df.GEOFLAG=='State']

    
    # merge with county data df to add county-level fips
    if 'COUNTY' in dfcounty:
        dfcounty = dfcounty.drop(columns='COUNTY') # some states have fips already; drop that
    if 'STATE' in dfcounty:
          dfcounty = dfcounty.drop(columns='STATE') # some states have fips already; drop that
    #dfcounty.COUNTY_NAME = dfcounty.COUNTY_NAME.astype(str)
    #dfcounty.STATE_NAME = dfcounty.STATE_NAME.astype(str)
    #df_county_fips.COUNTY_NAME = df_county_fips.COUNTY_NAME.astype(str)
    #df_county_fips.STATE_NAME = df_county_fips.STATE_NAME.astype(str)
    dfcounty = dfcounty.merge(df_county_fips, on=['COUNTY_NAME', 'STATE_NAME'], how='left')
        
    # add state fips to state data df
    #dfs = df_state[['state_name', 'statefips']]
    #dfs_dict = dict(zip(dfs['state_name'], dfs['statefips']))
    #dfc_dict = dict(zip(dfs['state_name'], 1000*dfs['statefips'].astype(int)))
    #dfstate['STATE'] = dfstate['STATE_NAME'].map(dfs_dict)
    #dfstate['COUNTY'] = dfstate['STATE_NAME'].map(dfc_dict)
    
    # combine state and county data again
    #df = pd.concat([dfcounty, dfstate], ignore_index=True)
    df = dfcounty.copy()
    
    # cleanup
    df = df.drop(columns = ['County', 'STATE_x','STATE_y' ], errors='ignore')

    return df, df_county_fips

####################################
def normalize_by_pop_agerace_county(df):
# https://www2.census.gov/programs-surveys/popest/technical-documentation/file-layouts/2010-2019/cc-est2019-alldata.pdf
# https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/counties/asrh/cc-est2019-alldata.csv
    
   df_norm = df[(df.CASE_TYPE == 'Partial Coverage') | (df.CASE_TYPE == 'Complete Coverage')]
   df_unnorm = df[(df.CASE_TYPE == 'Partial') | (df.CASE_TYPE == 'Complete')]

   # copy over the current unnormalized df and work on normalizing it
   df = df_unnorm.copy()
   df['CASE_TYPE'] = df['CASE_TYPE'].str.replace("Partial", "Partial Coverage")
   df['CASE_TYPE'] = df['CASE_TYPE'].str.replace("Complete", "Complete Coverage")


   ######################
   # population data from: https://www.census.gov/data/tables/time-series/demo/popest/2010s-state-detail.html
   popdf = pd.read_csv(drivelink+"/other_data/county_race_age_population.csv", encoding = "ISO-8859-1")

   #only keep data for 2019 pop estimate and for the total age group
   popdf = popdf[(popdf.YEAR==12)& (popdf.AGEGRP==0)]
   popdf.COUNTY = (popdf.STATE.astype(str).str.zfill(2)+popdf.COUNTY.astype(str).str.zfill(3)).astype(float)
      
   popdf['tot_pop'] = popdf.TOT_POP
   popdf['white_pop'] = popdf.WA_MALE + popdf.WA_FEMALE
   popdf['black_pop'] = popdf.BA_MALE + popdf.BA_FEMALE
   popdf['asian_pop'] = popdf.AA_MALE + popdf.AA_FEMALE + popdf.NA_MALE + popdf.NA_FEMALE
   popdf['native_pop'] = popdf.IA_MALE + popdf.IA_FEMALE
   popdf['hispanic_pop'] = popdf.H_MALE + popdf.H_FEMALE
   popdf['nothispanic_pop'] = popdf.NH_MALE + popdf.NH_FEMALE
   popdf['nothispanic_white_pop'] = popdf.NHWA_MALE + popdf.NHWA_FEMALE
   popdf['nothispanic_black_pop'] = popdf.NHBA_MALE + popdf.NHBA_FEMALE

   popdf = popdf[['STATE', 'COUNTY', 'tot_pop', 'white_pop', 'black_pop', 'asian_pop', 'native_pop', 'hispanic_pop', 'nothispanic_pop', 'nothispanic_white_pop', 'nothispanic_black_pop']]

   ############
   # merge
   popdf.COUNTY = popdf.COUNTY.astype(float)
   popdf.STATE = popdf.STATE.astype(float)
   df.COUNTY = df.COUNTY.astype(float)
   df.STATE = df.STATE.astype(float)

   df.loc[df.STATE_NAME == 'Texas', 'STATE'] = 48
   df = df.merge(popdf, on=['STATE','COUNTY'], how='left')
   df=df.replace('Suppressed', 'NaN')
   df['CASES_White'] = df['CASES_White'].astype(str).str.replace(',','').astype(float)
   df['CASES_Black'] = df['CASES_Black'].astype(str).str.replace(',','').astype(float)
   df['CASES_Asian'] = df['CASES_Asian'].astype(str).str.replace('Suppressed', 'NaN').astype(float)
   df['CASES_Asian'] = df['CASES_Asian'].astype(str).str.replace(',','').astype(float)
   df['CASES_Native'] = df['CASES_Native'].astype(str).str.replace(',','').astype(float)
   df['CASES_Hispanic'] = df['CASES_Hispanic'].astype(str).str.replace(',','').astype(float)
   if 'CASES_NotHispanic' in df:
       df['CASES_NotHispanic'] = df['CASES_NotHispanic'].astype(str).str.replace(',','').astype(float)
   df['CASES_OtherRace'] = df['CASES_OtherRace'].astype(str).str.replace(',','').astype(float)
   df['CASES_UnknownRace'] = df['CASES_UnknownRace'].astype(str).str.replace(',','').astype(float)
   if 'CASES_UnknownEthnicity' in df:
       df['CASES_UnknownEthnicity'] = df['CASES_UnknownEthnicity'].astype(str).str.replace(',','').astype(float)

   # normalize each column in df with population sizes
   df_hispasrace = df[df.Hispanic_as_Race == 1]
   df_hispnotasrace = df[df.Hispanic_as_Race != 1]

   df_hispasrace['CASES_White'] = 100*df_hispasrace['CASES_White']/df_hispasrace['nothispanic_white_pop'].astype(float)
   df_hispasrace['CASES_Black'] = 100*df_hispasrace['CASES_Black']/df_hispasrace['nothispanic_black_pop'].astype(float)
   df_hispasrace['CASES_Asian'] = 100*df_hispasrace['CASES_Asian']/df_hispasrace['asian_pop'].astype(float)
   df_hispasrace['CASES_Native'] = 100*df_hispasrace['CASES_Native']/df_hispasrace['native_pop'].astype(float)
   df_hispasrace['CASES_Hispanic'] = 100*df_hispasrace['CASES_Hispanic']/df_hispasrace['hispanic_pop'].astype(float)
   df_hispasrace['CASES_NotHispanic'] = float('NaN')

   df_hispnotasrace['CASES_White'] = 100*df_hispnotasrace['CASES_White']/df_hispnotasrace['white_pop'].astype(float)
   df_hispnotasrace['CASES_Black'] = 100*df_hispnotasrace['CASES_Black']/df_hispnotasrace['black_pop'].astype(float)
   df_hispnotasrace['CASES_Asian'] = 100*df_hispnotasrace['CASES_Asian']/df_hispnotasrace['asian_pop'].astype(float)
   df_hispnotasrace['CASES_Native'] = 100*df_hispnotasrace['CASES_Native']/df_hispnotasrace['native_pop'].astype(float)
   df_hispnotasrace['CASES_Hispanic'] = 100*df_hispnotasrace['CASES_Hispanic']/df_hispnotasrace['hispanic_pop'].astype(float)
   df_hispnotasrace['CASES_NotHispanic'] = 100*df_hispnotasrace['CASES_NotHispanic']/df_hispnotasrace['nothispanic_pop'].astype(float)

   df = pd.concat([df_hispasrace, df_hispnotasrace], ignore_index=True)


   # ADD CODE FOR CALCULATING UNKNOWNRACE UNKNOWNETHNICITY BASED ON HISPANIC AS RACE
   df = df[['STATE_NAME', 'STATE','COUNTY_NAME', 'COUNTY','DATE', 'CASE_TYPE', "CASES_White",'CASES_Black', 'CASES_Asian', 'CASES_Native', 'CASES_OtherRace', 'CASES_UnknownRace', 'CASES_Hispanic', 'CASES_NotHispanic', 'CASES_UnknownEthnicity', 'tot_pop', 'white_pop', 'black_pop', 'native_pop', 'asian_pop', 'hispanic_pop', 'nothispanic_pop', 'nothispanic_white_pop', 'nothispanic_black_pop']]

   ####################
   # combine the unormailzed dataframe with the normalized one
   df = pd.concat([df, df_norm], ignore_index=True)
    
   return df, popdf, df_unnorm

####################################
def unnormalize_by_pop_agerace_county(df):
# unnormalizes partial/complete coverage data by population size to get count data
    

    df_norm = df.copy()

    df['CASE_TYPE'] = df['CASE_TYPE'].str.replace("Partial Coverage", "Partial")
    df['CASE_TYPE'] = df['CASE_TYPE'].str.replace("Complete Coverage", "Complete")


    ######################
    # population data from: https://www.census.gov/data/tables/time-series/demo/popest/2010s-state-detail.html
    popdf = pd.read_csv(drivelink+"/other_data/county_race_age_population.csv", encoding = "ISO-8859-1")
    
    #only keep data for 2019 pop estimate and for the total age group
    popdf = popdf[(popdf.YEAR==12)& (popdf.AGEGRP==0)]
    popdf.COUNTY = (popdf.STATE.astype(str).str.zfill(2)+popdf.COUNTY.astype(str).str.zfill(3)).astype(float)
       
    popdf['tot_pop'] = popdf.TOT_POP
    popdf['white_pop'] = popdf.WA_MALE + popdf.WA_FEMALE
    popdf['black_pop'] = popdf.BA_MALE + popdf.BA_FEMALE
    popdf['asian_pop'] = popdf.AA_MALE + popdf.AA_FEMALE + popdf.NA_MALE + popdf.NA_FEMALE
    popdf['native_pop'] = popdf.IA_MALE + popdf.IA_FEMALE
    popdf['hispanic_pop'] = popdf.H_MALE + popdf.H_FEMALE
    popdf['nothispanic_pop'] = popdf.NH_MALE + popdf.NH_FEMALE
    popdf['nothispanic_white_pop'] = popdf.NHWA_MALE + popdf.NHWA_FEMALE
    popdf['nothispanic_black_pop'] = popdf.NHBA_MALE + popdf.NHBA_FEMALE
    
    popdf = popdf[['STATE', 'COUNTY', 'tot_pop', 'white_pop', 'black_pop', 'asian_pop', 'native_pop', 'hispanic_pop', 'nothispanic_pop', 'nothispanic_white_pop', 'nothispanic_black_pop']]

    ############
    # merge
    popdf.COUNTY = popdf.COUNTY.astype(float)
    popdf.STATE = popdf.STATE.astype(float)
    df.COUNTY = df.COUNTY.astype(float)
    df.STATE = df.STATE.astype(float)
    
    df = df.merge(popdf, on=['STATE','COUNTY'], how='left')

    df['CASES_White'] = df['CASES_White'].astype(str).str.replace(',','').astype(float)
    df['CASES_Black'] = df['CASES_Black'].astype(str).str.replace(',','').astype(float)
    df['CASES_Asian'] = df['CASES_Asian'].astype(str).str.replace(',','').astype(float)
    df['CASES_Native'] = df['CASES_Native'].astype(str).str.replace(',','').astype(float)
    df['CASES_Hispanic'] = df['CASES_Hispanic'].astype(str).str.replace(',','').astype(float)
    if 'CASES_NotHispanic' in df:
        df['CASES_NotHispanic'] = df['CASES_NotHispanic'].astype(str).str.replace(',','').astype(float)
    df['CASES_OtherRace'] = df['CASES_OtherRace'].astype(str).str.replace(',','').astype(float)
    df['CASES_UnknownRace'] = df['CASES_UnknownRace'].astype(str).str.replace(',','').astype(float)
    if 'CASES_UnknownEthnicity' in df:
        df['CASES_UnknownEthnicity'] = df['CASES_UnknownEthnicity'].astype(str).str.replace(',','').astype(float)
   
    # unnormalize each column in df with population sizes
    df_hispasrace = df[df.Hispanic_as_Race == 1]
    df_hispnotasrace = df[df.Hispanic_as_Race != 1]
    
    df_hispasrace['CASES_White'] = df_hispasrace['CASES_White']*df_hispasrace['nothispanic_white_pop'].astype(float)/100
    df_hispasrace['CASES_Black'] = df_hispasrace['CASES_Black']*df_hispasrace['nothispanic_black_pop'].astype(float)/100
    df_hispasrace['CASES_Asian'] = df_hispasrace['CASES_Asian']*df_hispasrace['asian_pop'].astype(float)/100
    df_hispasrace['CASES_Native'] = df_hispasrace['CASES_Native']*df_hispasrace['native_pop'].astype(float)/100
    df_hispasrace['CASES_Hispanic'] = df_hispasrace['CASES_Hispanic']*df_hispasrace['hispanic_pop'].astype(float)/100
    if 'CASES_NotHispanic' in df:
        df_hispasrace['CASES_NotHispanic'] = float('NaN')
    
    df_hispnotasrace['CASES_White'] = df_hispnotasrace['CASES_White']*df_hispnotasrace['white_pop'].astype(float)/100
    df_hispnotasrace['CASES_Black'] = df_hispnotasrace['CASES_Black']*df_hispnotasrace['black_pop'].astype(float)/100
    df_hispnotasrace['CASES_Asian'] = df_hispnotasrace['CASES_Asian']*df_hispnotasrace['asian_pop'].astype(float)/100
    df_hispnotasrace['CASES_Native'] = df_hispnotasrace['CASES_Native']*df_hispnotasrace['native_pop'].astype(float)/100
    df_hispnotasrace['CASES_Hispanic'] = df_hispnotasrace['CASES_Hispanic']*df_hispnotasrace['hispanic_pop'].astype(float)/100
    if 'CASES_NotHispanic' in df:
        df_hispnotasrace['CASES_NotHispanic'] = df_hispnotasrace['CASES_NotHispanic']*df_hispnotasrace['nothispanic_pop'].astype(float)/100

    df = pd.concat([df,df_hispasrace, df_hispnotasrace], ignore_index=True)
    
   
    # ADD CODE FOR CALCULATING UNKNOWNRACE UNKNOWNETHNICITY BASED ON HISPANIC AS RACE
    df = df[['STATE_NAME', 'STATE','COUNTY_NAME', 'COUNTY','DATE', 'CASE_TYPE', "CASES_White",                                                            
             'CASES_Black', 'CASES_Asian', 'CASES_Native',                                                             
             'CASES_OtherRace', 'CASES_UnknownRace',                                                            
             'CASES_Hispanic', 'CASES_NotHispanic',                                                             
             'CASES_UnknownEthnicity', ]]

    ####################
    # combine the unormailzed dataframe with the normalized one
    df = pd.concat([df, df_norm], ignore_index=True)

    return df

####################################
def add_countydata_to_states(df, df_fips):
       
    # get list of state fips where data is only available for states
    st_fips = list(df[df.GEOFLAG=='State']['STATE'].unique())
    
    for state in st_fips:
        
        county_fips = list(df_fips[df_fips.STATE== state]['COUNTY'].unique())
        county_names = list(df_fips[df_fips.STATE== state]['COUNTY_NAME'].unique())
        
        # get rows of state data for partial or complete coverage only
        df_state = df[(df.STATE == state) & ((df.CASE_TYPE == 'Partial Coverage') | (df.CASE_TYPE == 'Complete Coverage'))]
        
        # replicate state data for each county in that state
        for cf, cn in zip(county_fips, county_names):

            df_c = df_state.copy() # copy state data
            df_c['COUNTY'] = cf    # update county name, fips
            df_c['COUNTY_NAME'] = cn
            df = pd.concat([df,df_c]) # append it to main dataframe
                                    
    # get rid of entries for state level date (i.e. where COUNTY fips ends in 0000)
    # !!! This also drops the Partial and Complete (non normalized) state data
    #df = df[(df.COUNTY.astype(int).astype(str).str[-3:] != '000')]

    
    return df

###################################
def demo_data_fillunkown(df1):
    
    #####
    # FILL IN TOTALS
    df1['Total_adjusted'] = df1['Total vaccinated']
    df1['Total_adjusted'] = df1['Total_adjusted'].fillna(df1['CASES_Male']+df1['CASES_Female']+df1['CASES_UnknownGender'].fillna(0))
    df1['Total_adjusted'] = np.where(df1['Total_adjusted'] < df1['CASES_Male']+df1['CASES_Female']+df1['CASES_UnknownGender'].fillna(0), df1['CASES_Male']+df1['CASES_Female']+df1['CASES_UnknownGender'].fillna(0),df1['Total_adjusted'])

    #####
    # FILL IN Unknown gender
    df1['CASES_UnknownGender'] = np.where(~df1['CASES_Male'].isna() & ~df1['CASES_Female'].isna(),df1['Total_adjusted']- df1['CASES_Male']- df1['CASES_Female'],                                      df1['CASES_UnknownGender'])
    #####
    # FILL IN unkwnown race (combining with other race)
    # Assuming that unknownrace = otherrace (adding it in and then not subtracting from total for missing cases)
    
    df1['CASES_UnknownRace'] = df1['CASES_UnknownRace'].fillna(0) + df1['CASES_OtherRace'].fillna(0)
    
    conditions = [~df1["CASES_White"].isna() & ~df1['CASES_Black'].isna(), ~df1["CASES_White"].isna() & ~df1['CASES_Black'].isna() & ~df1['CASES_Asian'].isna(),~df1["CASES_White"].isna() & ~df1['CASES_Black'].isna() & ~df1['CASES_Asian'].isna() & ~df1['CASES_Native'].isna(), ~df1["CASES_White"].isna() & ~df1['CASES_Black'].isna() & ~df1['CASES_Asian'].isna() & ~df1['CASES_Native'].isna() & ~df1['CASES_Pacific'].isna()]
    choices = [df1['Total_adjusted']-df1["CASES_White"]-df1['CASES_Black'],              df1['Total_adjusted']-df1["CASES_White"]-df1['CASES_Black']-df1['CASES_Asian'],              df1['Total_adjusted']-df1["CASES_White"]-df1['CASES_Black']-df1['CASES_Asian']-df1['CASES_Native'],              df1['Total_adjusted']-df1["CASES_White"]-df1['CASES_Black']-df1['CASES_Asian']-df1['CASES_Native']-df1['CASES_Pacific']]
    df1['CASES_UnknownRace'] = np.select(conditions, choices, default=np.nan)
    
    
    ###
    # Fill in not hispanic
    if 'CASES_NotHispanic' in df:
        df1['CASES_NotHispanic'] = np.where(~df1['CASES_Hispanic'].isna() & df1['CASES_NotHispanic'].isna(),                                        df1['Total_adjusted']-df1['CASES_Hispanic'],                                        float('NaN'))
    
    #####
    # FILL IN unknown ethnicity
    df1['CASES_UnknownEthnicity'] = np.where(~df1['CASES_Hispanic'].isna() & ~df1['CASES_NotHispanic'].isna(),                                             df1['Total_adjusted']-df1['CASES_Hispanic'] - df1['CASES_NotHispanic'],                                             df1['Total_adjusted'])
    
    #####
    # FILL IN unkwno age
    conditions = [~df1['CASES_Adult'].isna() & ~df1['CASES_Elderly'].isna(),                 ~df1['CASES_Child'].isna() & ~df1['CASES_Adult'].isna() & ~df1['CASES_Elderly'].isna()]
    choices = [df1['Total_adjusted']- df1['CASES_Adult']- df1['CASES_Elderly'],               df1['Total_adjusted']-df1['CASES_Child'] - df1['CASES_Adult']- df1['CASES_Elderly']]
    df1['CASES_UnknownAge'] = np.select(conditions, choices, default=np.nan)
    
    #####
    # Clean up unknowns
    df1.loc[df1['CASES_UnknownRace'] < 0, 'CASES_UnknownRace'] = 0 # remove any negative values
    df1.loc[df1['CASES_UnknownEthnicity'] < 0, 'CASES_UnknownEthnicity'] = 0 # remove any negative values
    df1.loc[df1['CASES_UnknownAge'] < 0, 'CASES_UnknownAge'] = 0 # remove any negative values

    ######
    # normalize unknown cases by total cases (make % vaccinated rather % population)
    df1['CASES_UnknownGender'] = 100*df1['CASES_UnknownGender'].astype(float)/df1['Total_adjusted'].astype(float)
    df1['CASES_UnknownRace'] = 100*df1['CASES_UnknownRace'].astype(float)/df1['Total_adjusted'].astype(float)
    df1['CASES_UnknownEthnicity'] = 100*df1['CASES_UnknownEthnicity'].astype(float)/df1['Total_adjusted'].astype(float)
    df1['CASES_UnknownAge'] = 100*df1['CASES_UnknownAge'].astype(float)/df1['Total_adjusted'].astype(float)
    
    return df1

###################################
def cleanup_dfdemo(df):

    # make sure unknown values are 100% for states with missing data
    df['CASES_UnknownGender'] = np.where(df['CASES_Male'].isna() & df['CASES_Female'].isna(),
                                         100,df['CASES_UnknownGender'])
    df['CASES_UnknownRace'] = np.where(df['CASES_White'].isna() & df['CASES_Black'].isna()& df['CASES_Asian'].isna()& df['CASES_Native'].isna()& df['CASES_Pacific'].isna()& df['CASES_OtherRace'].isna(),
                                         100,df['CASES_UnknownRace'])
    df['CASES_UnknownAge'] = np.where(df['CASES_Child'].isna() & df['CASES_Adult'].isna()& df['CASES_Elderly'].isna(),
                                         100,df['CASES_UnknownAge'])
    df['CASES_UnknownEthnicity'] = np.where(df['CASES_Hispanic'].isna() & df['CASES_NotHispanic'].isna(),
                                         100,df['CASES_UnknownEthnicity'])
    
    # for states missing complete coverage data, add it
    states_partial = df[df.CASE_TYPE=='Partial Coverage'].STATE_NAME.unique()
    states_complete = df[df.CASE_TYPE=='Complete Coverage'].STATE_NAME.unique()
    states_missing_complete = [st for st in states_partial if st not in states_complete]
    cols =["CASES_Female", "CASES_Male", "CASES_UnknownGender","CASES_White",            'CASES_Black', 'CASES_Asian', 'CASES_OtherRace',             'CASES_Native', 'CASES_Pacific',            'CASES_Hispanic', 'CASES_NotHispanic',             'CASES_Child','CASES_Adult', 'CASES_Elderly',            'CASES_BlackDisparity', 'CASES_HispanicDisparity']
    cols2 = ["CASES_UnknownGender", 'CASES_UnknownRace', 'CASES_UnknownEthnicity', 'CASES_UnknownAge']
    for st in states_missing_complete:
        dfx = df[(df.CASE_TYPE == 'Partial Coverage') & (df.STATE_NAME == st)]
        dfx['CASE_TYPE'] = 'Complete Coverage'
        for col in cols:
            dfx[col] = float('NaN')
        for col in cols2:
            dfx[col] = 100
        df = pd.concat([df, dfx], ignore_index=True)
        
      
    # get rid of OtherRace data (we already combinedit into the Unknown category in the compile function)
    # Tried normalizing otherrace properly but couldn't
    df['CASES_OtherRace'] = np.nan
    
    # add disparity metric
    df['CASES_BlackDisparity'] = df['CASES_White']/df['CASES_Black']
    df['CASES_HispanicDisparity'] = df['CASES_NotHispanic']/df['CASES_Hispanic']
    
    # make wide format long
    df = pd.melt(df, id_vars=['STATE_NAME', 'STATE', "DATE", "CASE_TYPE"], value_vars=["CASES_Female", 
                                                                                       "CASES_Male", "CASES_UnknownGender","CASES_White",'CASES_Black', 'CASES_Asian', 'CASES_OtherRace','CASES_UnknownRace', 'CASES_Native', 'CASES_Pacific','CASES_Hispanic', 'CASES_NotHispanic','CASES_UnknownEthnicity',
                                                                                       'CASES_Child','CASES_Adult', 'CASES_Elderly', 'CASES_UnknownAge','CASES_BlackDisparity', 'CASES_HispanicDisparity'], var_name='DEMO_GROUP', value_name='CASES')
    
    # Fill in NAN with 0
    df = df.fillna(0)
    
    #print(df[df.duplicated()])
             
   
    # print list of states with demo data
    lt = df[(df.DEMO_GROUP=='CASES_White') & (df.CASES > 0)].STATE_NAME.unique()
    lt.sort()
    print("\n\nStates with demo data:", len(lt))
    df_state = pd.read_csv("../other_data/state_fips_abbrev.txt", sep="\t", header=None, names=['state_name', 'statefips', 'stabbrev'])
    abbrev = dict(zip(list(df_state.state_name), list(df_state.stabbrev)))
    for st in lt[:-1]:
        print(abbrev[st], ', ', end = '', sep='')
    print(abbrev[lt[-1]], '.', sep='')
    
    return df

##############################################
def clean_kff_state():
## NOTE ONLY RUNNING THIS ON ONE DATE FOR NOW (6/28/21)

    files = list(glob.glob(drivelink+'/input_files/demo_vacc/kff/raw_data-16.csv'))
    largedf=pd.DataFrame(columns=['STATE_NAME', 'Hispanic_as_Race', 'CASES_White', 'CASES_Black',
           'CASES_Hispanic', 'CASES_Asian', 'CASES_Native', 'CASES_OtherRace',
           'CASES_UnknownRace', 'CASES_UnknownEthnicity', 'DATE', 'CASE_TYPE',
           'GEOFLAG', 'PARTIAL_TOTAL_COUNT', 'STATE', 'KnownRace_Count',
           'KnownEthnicity_Count'])

    for file in files:
        try: 
            
            #print(file)
            
            df = pd.read_csv(file, skipfooter=29, skiprows=2)
            
            print(df.head())
            
            dfd = pd.read_csv(file, nrows=1, skiprows=1, header=None, names = ['date'])
            date = dfd['date'][0].split('of ')[1]
            df['DATE'] = pd.to_datetime(date, format = '%B %d, %Y')
            
            print(df.DATE)
            
            # drop uncessasry columns
            df = df.drop(columns=['% of Vaccinations with Known Race','% of Vaccinations with Known Ethnicity','Footnotes'])
            
            print("2: ", df.head())
            # rename columns
            df = df.rename(columns = {'Location':'STATE_NAME', 'Race Categories Include Hispanic Individuals':'Hispanic_as_Race',
               'White % of Vaccinations': 'CASES_White', 'Black % of Vaccinations': 'CASES_Black',
               'Hispanic % of Vaccinations':'CASES_Hispanic', 'Asian % of Vaccinations': 'CASES_Asian',
               'American Indian or Alaska Native % of Vaccinations': 'CASES_Native',
               'Native Hawaiian or Other Pacific Islander % of Vaccinations': 'CASES_Native2',
               'Other % of Vaccinations':'CASES_OtherRace', 
               '% of Vaccinations with Unknown Race': 'CASES_UnknownRace',
               '% of Vaccinations with Unknown Ethnicity': 'CASES_UnknownEthnicity'})
            

            # clean up df
            df = df.replace('<.01', float('NaN'))
            df = df.replace('NR', float('NaN'))
            df['CASE_TYPE'] = 'Partial'
            #df.CASES_Native = df.CASES_Native.astype(float).fillna(0) + df.CASES_Native2.astype(float).fillna(0)
            #df = df.drop(columns='CASES_Native2')
            df['GEOFLAG'] = 'State'
            df['Hispanic_as_Race'] = df.Hispanic_as_Race.replace('Yes', 1)
            df['Hispanic_as_Race'] = df.Hispanic_as_Race.replace('No', 0)
            
            print("4: ", df.head())
          
            # get total vaccination counts by state because the numbers provided are % of vaccinations
            dfs = pd.read_csv("https://data.cdc.gov/api/views/unsk-b7fc/rows.csv", usecols = [0,2,30])
            dfs = dfs.rename(columns={"Administered_Dose1_Recip": 'PARTIAL_TOTAL_COUNT','Date':'DATE', 'Location':'stabbrev'})
            dfs.DATE = pd.to_datetime(dfs.DATE)
            # add state full name
            df_state = pd.read_csv(drivelink+"/other_data/state_fips_abbrev.txt", sep="\t", header=None, names=['state_name', 'statefips', 'stabbrev'])
            dfs = dfs.merge(df_state, on= 'stabbrev')
            dfs = dfs.rename(columns={'state_name': 'STATE_NAME', 'statefips': 'STATE' })
            dfs = dfs.drop(columns='stabbrev')
                
            # add vaccination counts (note: vaccination percents reported are % of cases with known race/ethnicity)
            df = df.merge(dfs, on=['STATE_NAME', 'DATE'])
            print("5: ", df.head())

            df['KnownRace_Count'] = df.PARTIAL_TOTAL_COUNT.astype(float).fillna(0) * (1-df.CASES_UnknownRace.astype(float).fillna(0))
            df['KnownEthnicity_Count'] = df.PARTIAL_TOTAL_COUNT.astype(float).fillna(0) * (1-df.CASES_UnknownEthnicity.astype(float).fillna(0))
            df['CASES_count_White'] = df['CASES_White'].astype(float).fillna(0) * df['KnownRace_Count'].astype(float).fillna(0)
            df['CASES_count_Black'] = df['CASES_Black'].astype(float).fillna(0) * df['KnownRace_Count'].astype(float).fillna(0)
            df['CASES_count_Native'] = df['CASES_Native'].astype(float).fillna(0) * df['KnownRace_Count'].astype(float).fillna(0)
            df['CASES_count_Asian'] = df['CASES_Asian'].astype(float).fillna(0) * df['KnownRace_Count'].astype(float).fillna(0)
            df['CASES_count_Hispanic'] = df['CASES_Hispanic'].astype(float).fillna(0) * df['KnownEthnicity_Count'].astype(float).fillna(0)
            
            
            print("6:", df.head())

            #largedf=pd.concat([largedf,df])
        
        
        except: print()
        
        
    #newest_date=max(largedf['DATE'])
    #print('kff date: ',newest_date)
    
    #if one_date:
        #largedf=largedf.loc[largedf['DATE']==one_date]
    
    return df # NOTE THAT Vaccination coverage here is not what we might think? It might be proportion of all vacinated population as opposed to proportion of that group
    
##############################################
def clean_AL_demo():
    
    print('Alabama: only have 6/24/21')

    
    # if update file, update date
    df = pd.read_excel(drivelink+"/input_files/demo_vacc/AL/AL_county_demo_0624.xlsx", usecols = range(0,11)) # manually scraped
    
    df = df.rename(columns = {'County_name':'COUNTY_NAME', 'AI/AN': 'CASES_Native', 'Asian':'CASES_Asian',
                              'Black_AA': 'CASES_Black', 'NH_PI': 'CASES_Native2', '2_more':'CASES_OtherRace',
                              'White':'CASES_White', 'unk_race':'CASES_UnknownRace', 'Hispanic':'CASES_Hispanic',
                              'Not_hispanic':'CASES_NotHispanic', 'unk_eth':'CASES_UnknownEthnicity'})
    
    df.CASES_Native = df.CASES_Native.astype(float).fillna(0) + df.CASES_Native2.astype(float).fillna(0)
    df = df.drop(columns='CASES_Native2')
    df['COUNTY_NAME'] = df['COUNTY_NAME'].str.replace(' County', '')
    df['Hispanic_as_Race'] = 0
    df['CASE_TYPE'] = 'Partial Coverage'
    df['GEOFLAG'] ='County'
    df['STATE_NAME'] = 'Alabama'
    df['DATE'] = '2021-06-24'

    # add count data
    df, df_demo_fips = add_FIPS(df)
    df = unnormalize_by_pop_agerace_county(df)
    
    
    return df

#################################################
def clean_MA_demo():
    
    print('Massachusetts: only have 7/202/21')

    
    df = pd.read_excel(drivelink+"/input_files/demo_vacc/MA/Weekly-Municipality-COVID-19-Vaccination-Report-7-22-2021.xlsx", 
                       sheet_name='Race and Ethnicity - muni.', usecols = [0,2,5,8], skiprows=2, header=None,
                       names = ['County', 'RaceEthnicity', 'Individuals with at least one dose', 'Fully vaccinated individuals'])

    df = df.rename(columns = {'Individuals with at least one dose':"Partial", 'Fully vaccinated individuals': 'Complete'})
        
    # make case type data wide to long
    df = pd.melt(df, id_vars=['County', 'RaceEthnicity'], value_vars=["Partial", 'Complete'],             
                var_name='CASE_TYPE', value_name='CASES')
    df.CASES = df.CASES.replace('*', float('NaN'), regex=False)
    
    # groupby county    
    df = df.groupby(['County', 'RaceEthnicity', 'CASE_TYPE'])['CASES'].sum().reset_index()
    
    # make race/ethnicity long to wide
    df = df.pivot(index=['County', 'CASE_TYPE'], columns='RaceEthnicity', values='CASES').reset_index()
    df.columns.name = None # remove index name
    df = df.drop(columns = 'Total')
    
    df = df.rename(columns = {'County':'COUNTY_NAME', 'AI/AN': 'CASES_Native', 'Asian':'CASES_Asian',
                              'Black': 'CASES_Black', 'NH/PI': 'CASES_Native2','Multi':'CASES_OtherRace',
                              'White':'CASES_White', 'Other/Unknown':'CASES_UnknownRace', 'Hispanic':'CASES_Hispanic',
                              })
    
    df.CASES_Native = df.CASES_Native.astype(float).fillna(0) + df.CASES_Native2.astype(float).fillna(0)
    df = df.drop(columns='CASES_Native2')
    df['Hispanic_as_Race'] = 0
    df['GEOFLAG'] ='County'
    df['STATE_NAME'] = 'Massachusetts'

    return df

##############################################
def clean_CA_demo():
    
    print('California: have over time, only keeping 6/23/2021')
    
    link = 'https://data.chhs.ca.gov/dataset/e283ee5a-cf18-4f20-a92c-ee94a2866ccd/resource/262bffa8-c55b-478a-9906-222a3c5d6112/download/covid19vaccinesbycountybydemographic.csv'
    path=drivelink+'/input_files/demo_vacc/CA_county_data.csv'
    df = pd.read_csv(path, usecols = ['county', 'cumulative_at_least_one_dose', 
                                      'demographic_category', 'demographic_value', 
                                      'cumulative_fully_vaccinated', 'administered_date'])

    df = df.rename(columns = {'cumulative_at_least_one_dose':"Partial", 'cumulative_fully_vaccinated': 'Complete', 'administered_date': 'DATE'})
    df.DATE = pd.to_datetime(df.DATE, format='%Y-%m-%d')

    df = df[df.demographic_category == 'Race/Ethnicity']
    df = df.drop(columns='demographic_category')
        
    # make case type data wide to long
    df = pd.melt(df, id_vars=['county', 'DATE', 'demographic_value'], value_vars=["Partial", 'Complete'],             
                var_name='CASE_TYPE', value_name='CASES')

    # make race/ethnicity long to wide
    df = df.pivot(index=['county', 'DATE', 'CASE_TYPE'], columns='demographic_value', values='CASES').reset_index()
    df.columns.name = None # remove index name   

    df = df.rename(columns = {'county':'COUNTY_NAME', 'American Indian or Alaska Native': 'CASES_Native', 'Asian':'CASES_Asian',
                              'Black or African American': 'CASES_Black', 'Native Hawaiian or Other Pacific Islander': 'CASES_Native2', 'Multiracial':'CASES_OtherRace',
                              'White':'CASES_White', 'Latino':'CASES_Hispanic','Other Race': 'CASES_OtherRace2','Unknown': 'CASES_UnknownRace'
                              })

    df.CASES_Native = df.CASES_Native.astype(float).fillna(0) + df.CASES_Native2.astype(float).fillna(0)
    df.CASES_OtherRace = df.CASES_OtherRace.astype(float).fillna(0) + df.CASES_OtherRace2.astype(float).fillna(0)
    df = df.drop(columns=['CASES_Native2', 'CASES_OtherRace2'])

    df['Hispanic_as_Race'] = 0
    df['GEOFLAG'] ='County'
    df['STATE_NAME'] = 'California'

    # only keep latest data
    df_alldates = df.copy()
    df.loc[df.DATE == '2021-06-23 00:00:00', 'LATEST'] = 1
    df = df[df.LATEST == 1]
    df = df.drop(columns='LATEST')
    
    return df, df_alldates


############################################
def clean_IN_demo():
    
    print('Indiana')

    
    link = 'https://hub.mph.in.gov/dataset/145a43b2-28e5-4bf1-ad86-123d07fddb55/resource/82d99020-093f-41ac-95c7-d3c335b8c2ba/download/county-vaccination-demographics.xlsx'
    df = pd.read_excel(link, sheet_name = 'Race', usecols = ['county', 'fips', 'race', 'fully_vaccinated', 'current_as_of',
                                        'single_dose_administered'])
    df2 = pd.read_excel(link, sheet_name = 'Ethnicity', usecols = ['county', 'fips', 'ethnicity', 'fully_vaccinated', 'current_as_of',
                                        'single_dose_administered'])

    df = df.rename(columns = {'single_dose_administered':"Partial", 'fully_vaccinated': 'Complete', 'current_as_of': 'DATE'})
    df.DATE = pd.to_datetime(df.DATE).dt.date
    df2 = df2.rename(columns = {'single_dose_administered':"Partial", 'fully_vaccinated': 'Complete', 'current_as_of': 'DATE'})
    df2.DATE = pd.to_datetime(df2.DATE).dt.date
       
    # make case type data wide to long
    df = pd.melt(df, id_vars=['county', 'fips', 'DATE', 'race'], value_vars=["Partial", 'Complete'],             
                var_name='CASE_TYPE', value_name='CASES')
    df2 = pd.melt(df2, id_vars=['county', 'fips', 'DATE', 'ethnicity'], value_vars=["Partial", 'Complete'],             
                var_name='CASE_TYPE', value_name='CASES')

    # make race/ethnicity long to wide
    df = df.pivot(index=['county', 'fips', 'DATE','CASE_TYPE'], columns='race', values='CASES').reset_index()
    df.columns.name = None # remove index name
    df2 = df2.pivot(index=['county', 'fips', 'DATE','CASE_TYPE'], columns='ethnicity', values='CASES').reset_index()
    df2.columns.name = None # remove index name   

    df = df.rename(columns = {'county':'COUNTY_NAME', 'fips': 'COUNTY',
                              'Asian':'CASES_Asian',
                              'Black or African American': 'CASES_Black', 
                              'Other Race':'CASES_OtherRace',
                              'White':'CASES_White', 
                              'Unknown': 'CASES_UnknownRace'
                             })
    df2 = df2.rename(columns = {'county':'COUNTY_NAME', 'fips': 'COUNTY',
                             'Hispanic or Latino':'CASES_Hispanic', 'Not Hispanic or Latino': 'CASES_NotHispanic',
                             'Unknown': 'CASES_UnknownEthnicity'
                             })

    df = pd.concat([df, df2], ignore_index=True)

    df['Hispanic_as_Race'] = 0
    df['GEOFLAG'] ='County'
    df['STATE_NAME'] = 'Indiana'

    # only keep latest data
    df.loc[df.DATE == '2021-06-23', 'LATEST'] = 1
    df = df[df.LATEST == 1]
    df = df.drop(columns='LATEST')
    
    return df

####################################
def unnormalize_by_pop_county(df):
# unnormalizes partial/complete coverage data by population size to get count data
# ONLY NEEDED FOR COLORADO COUNTY (NON-DEMO) DATA
    
    # population data from: https://www.census.gov/data/tables/time-series/demo/popest/2010s-counties-total.html
    # Based on ACS 2019
    popdf = pd.read_csv(drivelink+"/other_data/county_population.csv", encoding = "ISO-8859-1")
    
    # get US population total before change df
    popt = popdf[popdf.COUNTY == 0] # only keep state entries
    popt = popt[['STNAME', 'POPESTIMATE2019']]
    
    popdf['COUNTY'] = (popdf['STATE'].astype(int)*1000 + popdf['COUNTY'].astype(int)).astype(float)
    popdf['POPN'] = popdf['POPESTIMATE2019']
    popdf = popdf[['COUNTY', 'POPN']]
    popdf = pd.concat([popdf, pd.DataFrame({'COUNTY':[46113], 'POPN':[14176]})], ignore_index=True)

    
    # clean up
    df = df[~df.CASE_TYPE.isna()]
    
    # add population data to vacc data based on fips code (works for state-level too)
    df.COUNTY = df.COUNTY.astype(float)
    df = df.merge(popdf, on = 'COUNTY', how='left')
           
    # calculate vaccination count;
    # copy over the df and add it in vertically
    df2 = df.copy()
    df2['CASES'] = ((df2['CASES'].astype(float).fillna(0)/100)*df2['POPN']).astype(int)
    df2['CASE_TYPE'] = df2['CASE_TYPE'].str.replace(' Coverage', '')
    df = pd.concat([df, df2], ignore_index=True)
    
    #cleanup
    df = df[['STATE_NAME', 'STATE', 'COUNTY_NAME', 'COUNTY','GEOFLAG', 'DATE', 'CASE_TYPE', 'CASES', 'POPN']]

    return df

####################################
def clean_CO_county():
    

    
    link = drivelink+'/input_files/county_vacc_Jul2021/Colorado/covid19_vaccine_2021-06-23.csv'
        
    df = pd.read_csv(link)
    df = df[['category', 'metric', 'value']]
    df = df[(df.category == '1+ Vaccination Rate') | (df.category == 'Up-to-Date Vaccination Rate')]
    df = df.rename(columns = {'category':'CASE_TYPE', 'metric':'COUNTY_NAME', 'value': 'CASES'})
    df.CASE_TYPE = df.CASE_TYPE.replace('1+ Vaccination Rate', 'Partial Coverage')
    df.CASE_TYPE = df.CASE_TYPE.replace('Up-to-Date Vaccination Rate', 'Complete Coverage')
    df.CASES = df.CASES*100

    #data is for multiple weeks, only keep latest
    df = df.groupby(['COUNTY_NAME', 'CASE_TYPE'])['CASES'].max().reset_index()
    dfmain = df.copy()
         
    # add counts by adding pop size
    dfmain['STATE_NAME'] = 'Colorado'
    dfmain['COUNTY_NAME'] = dfmain.COUNTY_NAME.str.replace(' County', '')
    dfmain['GEOFLAG'] = 'County'
    dfmain['DATE'] = '2021-07-22'
    dfmain, temp = add_FIPS(dfmain)
    dfmain = unnormalize_by_pop_county(dfmain)

    # only care about partial counts
    dfmain = dfmain[dfmain.CASE_TYPE == 'Partial']
    dfmain = dfmain.rename(columns={'CASES': 'TOTAL_PARTIAL_COUNT'})

    return dfmain

############################################
def clean_CO_demo():
    
    print('Colorado: have over time, only keeping 6/23/21')

    
    link = drivelink+'/input_files/county_vacc_Jul2021/Colorado/covid19_vaccine_2021-06-23.csv'
    
    df = pd.read_csv(link, usecols = ['section', 'category', 'metric', 'type', 'value'])
    df = df[df.section == 'County-level Data']
    df = df[df.category == 'Percent of Cumulative Vaccines by Demographics']
    df = df.drop(columns=['section', 'category'])

    df = df.rename(columns = {'metric':"COUNTY_NAME", 'value': 'CASES', 'type': 'DEMO_GROUP'})
    df = df[~df.DEMO_GROUP.isna()]
    demo_list = ['American Indian or Alaskan Native - Non Hispanic',
                 'Asian - Non Hispanic', 'Black or African American - Non Hispanic',
                 'Hispanic, All Races', 'Multi Race - Non Hispanic',
                 'Native Hawaiian or Other Pacific Islander - Non Hispanic',
                 'Other', 'Unknown', 'White - Non Hispanic']
    df = df[df.DEMO_GROUP.isin(demo_list)]
           
    # make race/ethnicity long to wide
    df = df.pivot(index=['COUNTY_NAME'], columns='DEMO_GROUP', values='CASES').reset_index()
    df.columns.name = None # remove index name   
    
    df = df.rename(columns = {'American Indian or Alaskan Native - Non Hispanic': 'CASES_Native', 'Asian - Non Hispanic':'CASES_Asian',
                              'Black or African American - Non Hispanic': 'CASES_Black', 'Native Hawaiian or Other Pacific Islander - Non Hispanic': 'CASES_Native2', 
                              'Multi Race - Non Hispanic':'CASES_OtherRace',
                              'White - Non Hispanic':'CASES_White', 'Hispanic, All Races':'CASES_Hispanic','Other': 'CASES_OtherRace2',
                              'Unknown': 'CASES_UnknownRace'
                             })
    
    df.CASES_Native = df.CASES_Native.astype(float).fillna(0) + df.CASES_Native2.astype(float).fillna(0)
    df.CASES_OtherRace = df.CASES_OtherRace.astype(float).fillna(0) + df.CASES_OtherRace2.astype(float).fillna(0)
    df = df.drop(columns=['CASES_Native2', 'CASES_OtherRace2'])
    
    df['CASES_NotHispanic'] = df.CASES_Native + df.CASES_Asian + df.CASES_Black + df.CASES_White
    
    
    # split data into county-scale and region-scale
    df_county = df[~df.COUNTY_NAME.str.contains('Region')] 
    df_region = df[df.COUNTY_NAME.str.contains('Region')]
    df.to_csv(drivelink+'/input_files/county_vacc_Jul2021/Colorado/temp1.csv')
    
    # make region level data into county-scale
    df_region = df_region.rename(columns = {'COUNTY_NAME':'REGION_NAME'})
    region_county_crosswalk = pd.read_csv(drivelink+'/input_files/county_vacc_Jul2021/Colorado/region_county_crosswalk_Naima.csv')
    region_county_crosswalk = region_county_crosswalk[region_county_crosswalk.COUNTY_NAME != region_county_crosswalk.REGION_NAME] # only keep crosswalk for regions
    region_county_crosswalk['REGION_NAME'] = region_county_crosswalk['REGION_NAME'] + ' Region'
    df_region = df_region.merge(region_county_crosswalk, on = 'REGION_NAME', how = 'right')
    df_region = df_region.drop(columns=['REGION ', 'REGION_NAME'])
   
    # put the county-level and region-level data back together
    df = pd.concat([df_county, df_region], ignore_index=True) 
        
    # make values into partial counts (provided as % of vaccinations)
    df_CO = clean_CO_county()
    df = df.merge(df_CO, on='COUNTY_NAME')
    df['CASES_Native'] = (df['CASES_Native']/100)*df.TOTAL_PARTIAL_COUNT
    df['CASES_Asian'] = (df['CASES_Asian']/100)*df.TOTAL_PARTIAL_COUNT
    df['CASES_Black'] = (df['CASES_Black']/100)*df.TOTAL_PARTIAL_COUNT
    df['CASES_White'] = (df['CASES_White']/100)*df.TOTAL_PARTIAL_COUNT
    df['CASES_OtherRace'] = (df['CASES_OtherRace']/100)*df.TOTAL_PARTIAL_COUNT
    df['CASES_Hispanic'] = (df['CASES_Hispanic']/100)*df.TOTAL_PARTIAL_COUNT
    df['CASES_NotHispanic'] = (df['CASES_NotHispanic']/100)*df.TOTAL_PARTIAL_COUNT
    

    df['Hispanic_as_Race'] = 0
    df['GEOFLAG'] ='County'
    df['STATE_NAME'] = 'Colorado'
    df['CASE_TYPE'] = 'Partial'
    df = df.drop(columns=['TOTAL_PARTIAL_COUNT', "POPN"])
    
    #counties = list(df.COUNTY_NAME.unique())
    
    return df

##############################################
def clean_CT_demo():
    
    print('Connecticut: have over time, only keeping 6/23/21')

    
    #link = 'https://data.ct.gov/api/views/wmiq-er83/rows.csv?accessType=DOWNLOAD'

    df = pd.read_csv(drivelink+'/input_files/demo_vacc/CT_county_data.csv')

    df = df.rename(columns = {'Date updated': 'DATE', 'Value': 'CASES', 'Race/ethnicity': 'DEMO_GROUP'})
    df['DATE']=pd.to_datetime(df['DATE'])

    df.loc[(df['Vaccination status']=='At least one dose') & (df['Data type']=='Count'), 'CASE_TYPE'] = 'Partial'
    df.loc[(df['Vaccination status']=='Fully vaccinated') & (df['Data type']=='Count'), 'CASE_TYPE'] = 'Complete'
    df = df[~df.CASE_TYPE.isna()]
    df = df.drop(columns=['Vaccination status', 'Data type'])

    # convert towns to counties and aggregate to county-level
    df_towncounty = pd.read_csv(drivelink+ "/input_files/demo_vacc/CT/CT_town_county.csv")
    df = df.merge(df_towncounty, on = 'Town name')
    df = df.groupby(['COUNTY_NAME', 'CASE_TYPE', 'DATE', 'DEMO_GROUP'])['CASES'].sum().reset_index()

    # drop unneeded demogroups
    df = df[df.DEMO_GROUP != 'Total']
       
    # make race/ethnicity long to wide
    df = df.pivot(index=['COUNTY_NAME', 'DATE', 'CASE_TYPE'], columns='DEMO_GROUP', values='CASES').reset_index()
    df.columns.name = None # remove index name    


    df = df.rename(columns = {'NH American Indian': 'CASES_Native', 'NH Asian or Pacific Islander':'CASES_Asian',
                              'NH Black': 'CASES_Black', 'Multiple Races':'CASES_OtherRace',
                              'NH White':'CASES_White', 'Hispanic':'CASES_Hispanic','NH Other Race': 'CASES_OtherRace2','Unknown Race': 'CASES_UnknownRace'
                             })

    df.CASES_OtherRace = df.CASES_OtherRace.astype(float).fillna(0) + df.CASES_OtherRace2.astype(float).fillna(0)
    df = df.drop(columns=['CASES_OtherRace2'])

    df['Hispanic_as_Race'] = 0
    df['GEOFLAG'] ='County'
    df['STATE_NAME'] = 'Connecticut'

    # only keep latest data
    df_alldates = df.copy()
    df.loc[df.DATE == '2021-06-23 00:00:00', 'LATEST'] = 1
    df = df[df.LATEST == 1]
    df = df.drop(columns='LATEST')
    
    return df, df_alldates


def normalize_by_pop(df_master_demo):
    
    lt = ['Out of State', 'Unknown', 'Suppressed']
    df_master_demo = df_master_demo[~df_master_demo.COUNTY.isin(lt)]
    df_master_demo=df_master_demo.loc[df_master_demo['CASE_TYPE']!='Both']
    df_master_demo = df_master_demo[~df_master_demo.COUNTY.isna()]
    df_master_demo.loc[df_master_demo.CASES_Black.isin(lt), 'CASES_Black'] = float('NaN')
    df_master_demo, popdf, df_master_unnorm = normalize_by_pop_agerace_county(df_master_demo)
    
    return df_master_demo, popdf, df_master_unnorm


def calc_disparity_metric(df_master_demo):
    
    df_master_demo['CASES_White'] = df_master_demo['CASES_White'].astype(str).str.replace(',','').astype(float)
    df_master_demo['CASES_Black'] = df_master_demo['CASES_Black'].astype(str).str.replace(',','').astype(float)
    df_master_demo.loc[(df_master_demo.CASE_TYPE=='Complete Coverage') & (df_master_demo.CASES_Black >0), 'Black_Disparity'] = df_master_demo.loc[(df_master_demo.CASE_TYPE=='Complete Coverage')  & (df_master_demo.CASES_Black >0), 'CASES_White']/df_master_demo.loc[(df_master_demo.CASE_TYPE=='Complete Coverage')  & (df_master_demo.CASES_Black >0), 'CASES_Black']
    df_master_demo.loc[(df_master_demo.CASE_TYPE=='Partial Coverage')  & (df_master_demo.CASES_Black >0), 'Black_Disparity'] = df_master_demo.loc[(df_master_demo.CASE_TYPE=='Partial Coverage')  & (df_master_demo.CASES_Black >0), 'CASES_White']/df_master_demo.loc[(df_master_demo.CASE_TYPE=='Partial Coverage')  & (df_master_demo.CASES_Black >0), 'CASES_Black']

    df_master_demo = df_master_demo.replace('Suppressed', float('NaN'))
    df_master_demo['CASES_Hispanic'] = df_master_demo['CASES_Hispanic'].astype(str).str.replace(',','').astype(float)
    df_master_demo.loc[(df_master_demo.CASE_TYPE=='Complete Coverage') & (df_master_demo.CASES_Hispanic >0), 'Hispanic_Disparity'] = df_master_demo.loc[(df_master_demo.CASE_TYPE=='Complete Coverage')  & (df_master_demo.CASES_Hispanic >0), 'CASES_White']/df_master_demo.loc[(df_master_demo.CASE_TYPE=='Complete Coverage')  & (df_master_demo.CASES_Hispanic >0), 'CASES_Hispanic']
    df_master_demo.loc[(df_master_demo.CASE_TYPE=='Partial Coverage')  & (df_master_demo.CASES_Hispanic >0), 'Hispanic_Disparity'] = df_master_demo.loc[(df_master_demo.CASE_TYPE=='Partial Coverage')  & (df_master_demo.CASES_Hispanic >0), 'CASES_White']/df_master_demo.loc[(df_master_demo.CASE_TYPE=='Partial Coverage')  & (df_master_demo.CASES_Hispanic >0), 'CASES_Hispanic']

    # CLEAN UP DISPARITY DATA
    df_master_demo['prop_black'] = df_master_demo.black_pop/df_master_demo.tot_pop
    df_master_demo['prop_hispanic'] = df_master_demo.hispanic_pop/df_master_demo.tot_pop
    df_master_demo['prop_white'] = df_master_demo.white_pop/df_master_demo.tot_pop
    df_master_demo.loc[(df_master_demo.black_pop < 250), 'Black_Disparity'] = float('NaN')
    df_master_demo.loc[(df_master_demo.prop_black < 0.02), 'Black_Disparity'] = float('NaN')
    df_master_demo.loc[(df_master_demo.Prop_unknown_to_black > 0.3), 'Black_Disparity'] = float('NaN')
    df_master_demo.loc[(df_master_demo.hispanic_pop < 250), 'Hispanic_Disparity'] = float('NaN')
    df_master_demo.loc[(df_master_demo.prop_hispanic < 0.02), 'Hispanic_Disparity'] = float('NaN')
    df_master_demo = df_master_demo.drop(columns = ['prop_black', 'prop_hispanic', 'prop_white' ])

    ###################################
    # ADD STATE LEVEL DISPARITY METRIC
    df_state_demo = df_master_demo.copy();
    df_state_demo.replace([np.inf, -np.inf], 0, inplace=True)
    df_state_demo['CASEcount_White'] = df_state_demo['CASES_White'].fillna(0)*df_state_demo['white_pop'].fillna(0)
    df_state_demo['CASEcount_Hispanic'] = df_state_demo['CASES_Hispanic'].fillna(0)*df_state_demo['hispanic_pop'].fillna(0)
    df_state_demo['CASEcount_Black'] = df_state_demo['CASES_Black'].fillna(0)*df_state_demo['black_pop'].fillna(0)

    df_state_demo = df_state_demo.groupby(['STATE', 'STATE_NAME', 'CASE_TYPE'])[['CASEcount_White','CASEcount_Black', 'CASEcount_Hispanic', 'white_pop', 'black_pop', 'hispanic_pop']].sum().reset_index()
    df_state_demo['Coverage_White'] = df_state_demo['CASEcount_White']/df_state_demo['white_pop']
    df_state_demo['Coverage_Black'] = df_state_demo['CASEcount_Black']/df_state_demo['black_pop']
    df_state_demo['Coverage_Hispanic'] = df_state_demo['CASEcount_Hispanic']/df_state_demo['hispanic_pop']

    df_state_demo['Black_Disparity'] = df_state_demo['Coverage_White']/df_state_demo['Coverage_Black']
    df_state_demo['Hispanic_Disparity'] = df_state_demo['Coverage_White']/df_state_demo['Coverage_Hispanic']

    df_state_demo['Black_Disparity'] = df_state_demo['Black_Disparity'].replace([np.inf, -np.inf], float('NaN'))
    df_state_demo['Hispanic_Disparity'] = df_state_demo['Hispanic_Disparity'].replace([np.inf, -np.inf], float('NaN'))
    
    # Add state abbreviation
    dfa = pd.read_csv(drivelink+"/other_data/state_fips_abbrev.txt", sep="\t", header=None, names=['STATE_NAME', 'STATE', 'STATE_ABBREV'])
    dfa = dfa.drop(columns = ['STATE_NAME'])
    df_state_demo.STATE = df_state_demo.STATE.astype(float).astype(int)
    df_state_demo = df_state_demo.merge(dfa, on='STATE')
    
    return df_master_demo, df_state_demo

def add_stats_about_unknown(df_master_demo):
    
    df_master_demo['casecount_white'] = df_master_demo.CASES_White.fillna(0)*df_master_demo.white_pop
    df_master_demo['casecount_black'] = df_master_demo.CASES_Black.fillna(0)*df_master_demo.black_pop
    df_master_demo['casecount_hispanic'] = df_master_demo.CASES_Hispanic.fillna(0)*df_master_demo.hispanic_pop
    df_master_demo['casecount_asian'] = df_master_demo.CASES_Asian.fillna(0)*df_master_demo.asian_pop
    df_master_demo['casecount_native'] = df_master_demo.CASES_Native.fillna(0)*df_master_demo.native_pop
    df_master_demo['unknown_count'] = df_master_demo['CASES_OtherRace'].fillna(0)+df_master_demo['CASES_UnknownRace'].fillna(0) + df_master_demo['CASES_UnknownEthnicity'].fillna(0)
    df_master_demo['casecount_total'] = df_master_demo.casecount_white+df_master_demo.casecount_black+df_master_demo.casecount_hispanic + df_master_demo.casecount_asian + df_master_demo.casecount_native + df_master_demo.unknown_count
    df_master_demo['Prop_unknown'] = df_master_demo.unknown_count/df_master_demo.casecount_total
    df_master_demo['Prop_unknown_to_black'] = df_master_demo.unknown_count/df_master_demo.casecount_black
    df_master_demo = df_master_demo.drop(columns = ['casecount_white', 'casecount_black', 'casecount_hispanic', 'casecount_asian', 'casecount_native','unknown_count', 'casecount_total', ])

    return df_master_demo

########################################################################
########################################################################
########################################################################
########################################################################
# MAIN CODE
########################################################################
########################################################################
########################################################################
########################################################################


state_names = ["Hawaii", "Alaska", "Alabama", "Arkansas", "Arizona", "California", "Colorado",                
               "Connecticut", "District of Columbia", "Delaware", "Florida",                
               "Georgia", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas",                
               "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine",                
               "Michigan", "Minnesota", "Missouri", "Mississippi", "Montana",                
               "North Carolina", "North Dakota", "Nebraska", "New Hampshire",               
               "New Jersey", "New Mexico", "Nevada", "New York", "Ohio",               
               "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",               
               "South Dakota", "Tennessee", "Texas", "Utah", "Virginia",  "Vermont",               
               "Washington", "Wisconsin", "West Virginia", "Wyoming"]

demo_data_states = []
df_master_demo = pd.DataFrame()


# FIX NATIVE

####################################
# Step 1: add states for which there is only state data (from KFF)
####################################
#df_kff = clean_kff_state()
#df_master_demo = pd.concat([df_master_demo, df_kff], ignore_index=True) #partial


###################################
# Step 2: grab all states with file downloadable data for county-level data
####################################
# States with downloadable files 
df_TX = clean_TX_demo() #partial, complete

df_TN = clean_TN_demo() #partial

df_MO = clean_MO_demo('06-25') #partial, complete

df_LA = clean_LA_demo('06-25') #partial, complete

df_GA = clean_GA_demo('06-25') #partial

df_NC = clean_NC_demo() #partial, complete

#df_PA = clean_PA_demo() #partial, complete -- Mar2024- only current so not usable

df_MA = clean_MA_demo() # partial, complete

df_CA, df_CA_alldates = clean_CA_demo() # partial, complete

df_CO = clean_CO_demo() # partial, complete

df_WV = clean_WV_demo('06-25') #partial

df_CT, df_CT_alldates = clean_CT_demo() #partial, comp

# add states without fips & add fips code to them
df_master_demo=pd.concat([df_CA,df_TN, df_MO, df_LA, df_GA,df_NC, df_WV, df_MA, df_CO,df_CT,#df_PA,
                          df_TX], ignore_index=True)
df_master_demo, df_county_fips = add_FIPS(df_master_demo)
df_master_CA_alldates, temp = add_FIPS(df_CA_alldates)

#add VA which already has fips -- Mar2024-- link not working --> done
df_VA = clean_VA_demo(df_county_fips)
df_master_demo = pd.concat([df_master_demo, df_VA], ignore_index=True)

# add AL which already has fips
df_AL = clean_AL_demo()
df_master_demo = pd.concat([df_master_demo, df_AL], ignore_index=True)

# add IN which already has fips -- Mar2024- only has latest data
#df_IN = clean_IN_demo()
#df_master_demo = pd.concat([df_master_demo, df_IN], ignore_index=True)

dfx = df_master_demo[df_master_demo.STATE_NAME =='Massachusetts']

####################################
# Step 3: normalize, make state data county data
####################################
# normalize by popsize
df_master_demo, popdf, df_master_unnorm = normalize_by_pop(df_master_demo)
df_master_CA_alldates['CASES_NotHispanic'] = float('NaN')
df_master_CA_alldates['CASES_UnknownEthnicity'] = float('NaN')
df_master_CA_alldates, popdf_CA, df_master_CA_alldates_unnorm = normalize_by_pop(df_master_CA_alldates)


# for all data at state-level (from kff), make it county level
#df_master_demo = add_countydata_to_states(df_master_demo, df_demo_fips)

# Add statistics about unknown proportion
df_master_demo = add_stats_about_unknown(df_master_demo)
df_master_CA_alldates = add_stats_about_unknown(df_master_CA_alldates)

####################################
# Add disparity metric
df_master_demo, df_state_demo = calc_disparity_metric(df_master_demo)
df_master_CA_alldates, temp = calc_disparity_metric(df_master_CA_alldates)


####################################
# cleanup
df_master_demo.DATE = pd.to_datetime(df_master_demo.DATE, format = '%Y-%m-%d')
df_master_demo = df_master_demo.drop(columns = ['LATEST', 'GEOFLAG', 'TOTAL'])
df_master_demo.STATE = df_master_demo.STATE.astype(int)
df_master_demo.COUNTY = df_master_demo.COUNTY.astype(int)

####################################
# Step 8: output master data
####################################
# output current version
df_master_demo.to_csv(drivelink+'/COVID_Vacc_Demo_Data/data_demo_normalized.csv', index=False)
df_master_unnorm.to_csv(drivelink+'/COVID_Vacc_Demo_Data/data_demo_not_normalized.csv', index=False)
df_state_demo.to_csv(drivelink+'/COVID_Vacc_Demo_Data/data_demo_state_level.csv', index=False)
df_master_CA_alldates.to_csv(drivelink+'/COVID_Vacc_Demo_Data/data_demo_normalized_overtime_CA.csv', index=False)

# keep dated version as backup
df_master_demo.to_csv(drivelink+"/output_files/data_master_demo_"+time.strftime("%Y%m%d")+".csv", index=False)

#print("--- %s seconds ---" % (time.time() - start_time))
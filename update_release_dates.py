# -*- coding: utf-8 -*-
"""
Created on Mon Jul 25 11:25:48 2016

@author: e612727
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime as dt

def update_release_dates(df, transformation, bdays=1, early_release=False):
    
    def compute_bdays(x):
        if pd.isnull(x['ECO_RELEASE_DT']):
            return np.nan
        elif x['Date'] > x['ECO_RELEASE_DT']:
            #if early_release:
                return pd.date_range(x['ECO_RELEASE_DT']+pd.tseries.offsets.BDay(n=1), 
                                 x['Date']+pd.tseries.offsets.BDay(n=0), 
                                 freq='B').size
#            else:    
#                return np.nan
        else:
            return pd.date_range(x['Date']+pd.tseries.offsets.BDay(n=1), 
                                 x['ECO_RELEASE_DT']+pd.tseries.offsets.BDay(n=0), 
                                 freq='B').size
       
    
    def apply_bday_offset(x):
        if early_release:
            return x['Date'] - x['bdays'] * pd.tseries.offsets.BDay()
        else:
            return x['Date'] + x['bdays'] * pd.tseries.offsets.BDay()
    
    
    # change the format of the two dates to datetime ; drop all values of NaN in Actual_release 
    n=len(df)
    df['Date']=pd.to_datetime(df['Date'])
    df1 = df.dropna(how='all',subset= ['ECO_RELEASE_DT'])
    df2 = df[:n-len(df1)-2]
    df1.ECO_RELEASE_DT=df1.ECO_RELEASE_DT.astype(int)
    df1.ECO_RELEASE_DT=df1.ECO_RELEASE_DT.astype(str)
    df1['ECO_RELEASE_DT']=pd.to_datetime(df1['ECO_RELEASE_DT'])
    df['RDate']=0
    if df['ECO_RELEASE_DT'].isnull().all():
        print('No release dates found. Applying %d b-day increment to reference date to compute release dates' % bdays)
        df['bdays'] = bdays
        df['RDate']=df['Date'] + bdays* pd.tseries.offsets.BDay()
    else:
        # ffill and bfill the b-day difference
        df1['bdays'] = df1.apply(compute_bdays,axis=1)
        df1['RELEASE_DT'] = df1.apply(apply_bday_offset, axis=1)
        m=int(df1['bdays'].mean())
  
#        df['bdays'] = df['bdays'].ffill()
#        df['bdays'] = df['bdays'].bfill()
        l=tuple(df1.index)
        
        for i in range(0,n):
            if i in l:
            
                df['RDate'][i]= df1['RELEASE_DT'][i]
            else:
             
                df['RDate'][i]=df['Date'][i] + m * pd.tseries.offsets.BDay()
 
    # combine the actual release and px_last with the index being the update release date
    df = df[['PX_LAST','ACTUAL_RELEASE']].dropna(how='all').set_index(df['RDate'])
    df['PX_BLENDED'] = df['ACTUAL_RELEASE'].combine_first(df['PX_LAST'])
    df.dropna(how='all')
   
    g=pd.DataFrame(df.index).duplicated() 
    for i in range(1,n):
       if g[i]==True:
           tt = df[df[~i]]
           
    if transformation=="Diff1M":
        df['Diff1M']=pd.DataFrame(df['PX_BLENDED'].diff()) 
    else:
        df['Diff1M']=0
  
         
    return df


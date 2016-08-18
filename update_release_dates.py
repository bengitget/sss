# -*- coding: utf-8 -*-
"""
Created on Mon Jul 25 11:25:48 2016

@author: e612727
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime as dt

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime as dt

def update_release_dates(df, transformation, bdays=1):
    
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
       
    
#    
    # change the format of the two dates to datetime ; drop all values of NaN in Actual_release 
    df1=df.dropna(subset=['PX_LAST'])
    df1['Date']=pd.to_datetime(df1['Date'])
    
    df1 = df.dropna(how='all',subset= ['ECO_RELEASE_DT'])
    num=len(df)
    df1.ECO_RELEASE_DT=df1.ECO_RELEASE_DT.astype(int)
    
    df1.ECO_RELEASE_DT=df1.ECO_RELEASE_DT.astype(str)
    df1['ECO_RELEASE_DT']=pd.to_datetime(df1['ECO_RELEASE_DT'])
    
    if df['ECO_RELEASE_DT'].isnull().all():
        print('No release dates found. Applying %d b-day increment to reference date to compute release dates' % bdays)
        df['bdays'] = bdays
        df['RDate']=df['Date'] + bdays* pd.tseries.offsets.BDay()
        m=bdays
    else:
        # ffill and bfill the b-day difference
        df1['bdays'] = df1.apply(compute_bdays,axis=1)
        m=int(df1['bdays'].mean())
        df['RRDate']= df1['ECO_RELEASE_DT']
        df['RRRDate']=df['Date'] + m * pd.tseries.offsets.BDay()
        df['RDate'] = df['RRDate'].combine_first(df['RRRDate'])
        
    g=pd.DataFrame(df['RDate']).duplicated() 
#    for i in range(2,num+1):
#       if g[i]==True:
    f=g[g==True].index
    df = df.drop((df.index[f-2]))
    df = df.dropna(how='all',subset= ['PX_LAST','ACTUAL_RELEASE'])
    # combine the actual release and px_last with the index being the update release date
    df = df[['PX_LAST','ACTUAL_RELEASE']].dropna(how='all').set_index(df['RDate'])
    df['PX_BLENDED'] = df['ACTUAL_RELEASE'].combine_first(df['PX_LAST'])
    df.dropna(how='all')
   
           
    if transformation=="Diff1M":
        df['Diff1M']=pd.DataFrame(df['PX_BLENDED'].diff()) 
    elif transformation=="Ndiff1M":
        df['Diff1M']=pd.DataFrame(df['PX_BLENDED'].diff()*(-1))
    elif transformation=="Level":
        df['Diff1M']=df['PX_BLENDED']
    elif transformation=="NLevel":
        df['Diff1M']=df['PX_BLENDED']*(-1)
    elif transformation == "log":
       log_n= np.log(df['PX_BLENDED'].astype('float64'))
       df['Diff1M']=pd.DataFrame(log_n).diff()
    elif transformation =="rolling12M":
       df['Diff1M']=pd.rolling_sum(pd.DataFrame(df['PX_BLENDED']),12).diff()
    elif transformation == 'logsign':
        
        tem=[]
        for i in range(0,len(df['PX_BLENDED'])):
          
            if df['PX_BLENDED'][i] > 0:
                tem.append(df['PX_BLENDED'][i]*10)
                i=i+1
            else:
                tem.append(-df['PX_BLENDED'][i]*10)
                i=i+1
        tem=pd.DataFrame(tem, index=df.index)
        log_n=np.log(tem)
        df['Diff1M']=pd.DataFrame(log_n).diff()
    else:
        df['Diff1M']=0
       

    return df,m
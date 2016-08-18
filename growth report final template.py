
# coding: utf-8

# In[ ]:




# In[18]:

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
    else:
        df['Diff1M']=0
       

    return df,m  

    return df,m



def usalphaIC (release, sign, date_range):

    smth_halflife=3
    zsc_halflife=6
    subtract_mean=True
    score_cap=3
    score_lag=0
    zscore=[]
    cparams = {}
    
    
    
    
    
    k=pd.DataFrame(release)
    dt_range=date_range
    
 
        
    score_df=calc_zscore(k,
                         mean_halflife=zsc_halflife, 
                         std_halflife=zsc_halflife, 
                         mean_seed_period=zsc_halflife,
                         std_seed_period=zsc_halflife,
                         smth_halflife=smth_halflife,
                         subtract_mean=subtract_mean,
                         cap=score_cap, 
                         lag=score_lag)
  
    
    score_df1 = score_df.reindex(index=dt_range)

    score_df1 = score_df1.ffill(axis=0)
    
   
    final_z = score_df1.shift(2)
    if sign==0:
        final_z=final_z*(-1)
    else:
        final_z=final_z
       
   
    return final_z      


# In[3]:

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import datetime as dt
import xlwt
from eureka.risk import calc_ewma_riskmodel
from eureka.signal import calc_zscore, score_to_alpha
from eureka.optimize import mean_variance_optimizer
from eureka.backtest import backtest_metrics
from eureka.report import backtest_report, aggregate_report
from inflaZ import inflaZ
from eureka.backtest import calc_realized_IC
start_dt = dt.datetime(1999, 1, 1)
end_dt =  dt.datetime(2016, 4, 29)
freq = 'B' # business-days
dt_range_rates = pd.date_range(start=start_dt, end=end_dt, freq=freq, normalize=True) #for futures rates
dt_range_fx = pd.date_range(start=start_dt, end=dt.datetime(2015,12,17), freq=freq, normalize=True) #for fx
cparams = {}

# strategy
cparams['strategy'] = 'DMFX'

# asset list
cparams['assets'] = ['AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'NOK', 'NZD', 'SEK']
cparams['assets_with_USD'] = ['AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'NOK', 'NZD', 'SEK', 'USD']
cparams['forward_tenor'] = '3M'
rmparams = {}
rmparams['vol_halflife'] = 21
rmparams['corr_halflife'] = 126
rmparams['corr_cap'] = 1.0
rmparams['corr_type'] = 'shrinktoaverage'
rmparams['lag'] = 0   


# In[11]:

rates_return=pd.read_csv('rates_futures_returns.csv')
dmfx=pd.read_csv('dmfx_returns.csv')


fields_rates= [['AUD_3Y','AUD_10Y'], ['CAD_10Y'], ['GER_2Y','GER_5Y','GER_10Y','GER_30Y','FRA_10Y'], ['GBP_2Y','GBP_10Y'], ['JPY_10Y'], ['USD_2Y','USD_5Y','USD_10Y','USD_20Y','USD_30Y']]





fx=dmfx[cparams['assets']]
fx=pd.DataFrame(fx)
fx.index=dt_range_fx
riskmodel_dict_fx = calc_ewma_riskmodel(fx,
                          vol_halflife=rmparams['vol_halflife'],
                          vol_seed_period=rmparams['vol_halflife'],
                           corr_halflife=rmparams['corr_halflife'],
                            corr_seed_period=rmparams['corr_halflife'],
                            corr_type=rmparams['corr_type'],
                            corr_cap=rmparams['corr_cap'],
                            lag=rmparams['lag'])


cparams['assets in FI futures']=['AUD_3Y','AUD_10Y','CAD_10Y','GBP_2Y','GBP_10Y','USD_2Y','USD_5Y','USD_10Y','USD_20Y','USD_30Y','JPY_10Y','GER_2Y','GER_5Y','GER_10Y','GER_30Y','FRA_10Y']
futures_rates=rates_return[cparams['assets in FI futures']]
futures_rates.index=dt_range_rates
riskmodel_dict_rates = calc_ewma_riskmodel(futures_rates,
                          vol_halflife=rmparams['vol_halflife'],
                          vol_seed_period=rmparams['vol_halflife'],
                           corr_halflife=rmparams['corr_halflife'],
                            corr_seed_period=rmparams['corr_halflife'],
                            corr_type=rmparams['corr_type'],
                            corr_cap=rmparams['corr_cap'],
                            lag=rmparams['lag'])

data=pd.ExcelFile('update growth data.xlsx',na_values=['NA'])
au=data.parse(0)
canada=data.parse(1)
nz=data.parse(2) # up to 33
norway=data.parse(3)
sweden=data.parse(4)
switz=data.parse(5)
uk=data.parse(6)
japan=data.parse(7) # up to 33
eu=data.parse(8)
us=data.parse(9)

################

      update,m=update_release_dates(input,transformation=transf.iloc[ii,i])
        z=usalphaIC(release=update['Diff1M'],sign=1,date_range=dt_range_rates)
#        ic.append(result)
#        mt.append(m)
        zscore.append(z)

#    au_z=zscore    

    z2.append(zscore)

z=[]
for i in range(0,6):
    z2_m=pd.concat(z2[i],axis=1).mean(axis=1)
    z.append(z2_m)
total_m=pd.concat(z,axis=1)
z=total_m
z.columns=['AU', 'CA', 'EU', 'UK', 'JP', 'US']
z=z*(-1)
au_z=pd.concat([z['AU'],z['AU']],axis=1)
au_z.columns=['AUD_3Y','AUD_10Y']

ca_z=pd.DataFrame(z['CA'])
ca_z.columns=['CAD_10Y']

eu_z=pd.concat([z['EU'],z['EU'],z['EU'],z['EU'],z['EU']],axis=1)
eu_z.columns=['GER_2Y','GER_5Y','GER_10Y','GER_30Y','FRA_10Y']

uk_z=pd.concat([z['UK'],z['UK']],axis=1)
uk_z.columns=['GBP_2Y','GBP_10Y']

jp_z=pd.DataFrame(z['JP'])
jp_z.columns=['JPY_10Y']
                
us_z=pd.concat([z['US'],z['US'],z['US'],z['US'],z['US']],axis=1)
us_z.columns=['USD_2Y','USD_5Y','USD_10Y','USD_20Y','USD_30Y']

total=pd.concat([au_z,ca_z,uk_z,us_z,jp_z,eu_z],axis=1)




# In[ ]:


z=total_m
z.columns=['AU', 'CA', 'EU', 'UK', 'JP', 'US']
z=z*(-1)
au_z=pd.concat([z['AU'],z['AU']],axis=1)
au_z.columns=['AUD_3Y','AUD_10Y']

ca_z=pd.DataFrame(z['CA'])
ca_z.columns=['CAD_10Y']

eu_z=pd.concat([z['EU'],z['EU'],z['EU'],z['EU'],z['EU']],axis=1)
eu_z.columns=['GER_2Y','GER_5Y','GER_10Y','GER_30Y','FRA_10Y']

uk_z=pd.concat([z['UK'],z['UK']],axis=1)
uk_z.columns=['GBP_2Y','GBP_10Y']

jp_z=pd.DataFrame(z['JP'])
jp_z.columns=['JPY_10Y']
          
us_z=pd.concat([z['US'],z['US'],z['US'],z['US'],z['US']],axis=1)
us_z.columns=['USD_2Y','USD_5Y','USD_10Y','USD_20Y','USD_30Y']

total=pd.concat([au_z,ca_z,uk_z,us_z,jp_z,eu_z],axis=1)


# In[24]:





# In[ ]:

fields_fx= ['AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'NOK', 'NZD', 'SEK']

country_fx=[au,canada,switz,eu,uk,japan,norway,nz,sweden]
transf=pd.read_excel('g10_G_transformation.xlsx')

z=[]
z2=[]
for i in range(0,9):
    
    cc=country[i]
    n=int((len(cc.columns)+1)/5)
    zscore=[]
    input=[]
   
    for ii in range(0,n):
        
        input=cc.iloc[2:,5*ii:5*ii+4]
         
        input=cc.iloc[2:,5*ii:5*ii+4]
        input.columns=['Date','PX_LAST','ECO_RELEASE_DT','ACTUAL_RELEASE']
        
        update,m=update_release_dates(input,transformation=transf.iloc[ii,i])
        z=usalphaIC(release=update['Diff1M'])
#        ic.append(result)
#        mt.append(m)
        zscore.append(z)

#    au_z=zscore    

    z2.append(zscore)

z=[]
for i in range(0,6):
    z2_m=pd.concat(z2[i],axis=1).mean(axis=1)
    z.append(z2_m)
total_m=pd.concat(z,axis=1)

holdingdict = mean_variance_optimizer(alpha_1, 
                                                   riskmodel_dict['cov'], 
                                                   risk_aversion = cparams['opto_params']['risk_aversion'], 
                                                   position_lower_bound = cparams['opto_params']['position_lower_bound'],
                                                   position_upper_bound = cparams['opto_params']['position_upper_bound'])
                                                       
      
backtestdict = {}
    
        
backtestdict = backtest_metrics(fx,
                                                 holdingdict,
                                                 riskmodel_dict,
                                                 z,
                                                 alpha_1,   
                                                 lead_lag_period=21,
                                                 risk_aversion=cparams['opto_params']['risk_aversion'])


aggregate_report(backtestdict,
                     title='G9 signal',
                     author='AEG',
                     filename='FX Growth Result with GDP'+'.pdf'


# In[21]:

l=pd.DataFrame(riskmodel_dict_rates['vol']) 
alpha_1 = score_to_alpha(total, l, IC = 0.1)



cparams = {}

# strategy
cparams['strategy'] = 'DMFX'

# asset list
cparams['assets'] = ['AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'NOK', 'NZD', 'SEK']
cparams['assets_with_USD'] = ['AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'NOK', 'NZD', 'SEK', 'USD']
cparams['forward_tenor'] = '3M'
optoparams = {}
optoparams['riskmodel'] = 'SHRINKAVG'
optoparams['risk_aversion'] = 200
optoparams['risk_aversion_tcost'] = 85
optoparams['tcost_aversion'] = 0.05
optoparams['position_lower_bound'] = -1.0
optoparams['position_upper_bound'] = 1.0
optoparams['portfolio_lower_bound'] = -0.25
optoparams['portfolio_upper_bound'] = 0.25
cparams['opto_params'] = optoparams
holdingdict = {}  

holdingdict = mean_variance_optimizer(alpha_1, 
                                                   riskmodel_dict['cov'], 
                                                   risk_aversion = cparams['opto_params']['risk_aversion'], 
                                                   position_lower_bound = cparams['opto_params']['position_lower_bound'],
                                                   position_upper_bound = cparams['opto_params']['position_upper_bound'])
                                                       
      
backtestdict = {}
    
        
backtestdict = backtest_metrics(futures_rates,
                                                 holdingdict,
                                                 riskmodel_dict,
                                                 total,
                                                 alpha_1,   
                                                 lead_lag_period=21,
                                                 risk_aversion=cparams['opto_params']['risk_aversion'])


aggregate_report(backtestdict,
                     title='G9 signal',
                     author='AEG',
                     filename='test trial'+'pdf')
                 #'G10 Futures on Rates Growth Result s=3 z=6 Final Report'+'.pdf')



# In[20]:

holdingdict


# In[ ]:

country=[au,canada,eu,uk,japan,us]
frequency=pd.read_excel('frequency.xlsx')
sector=pd.read_excel('sector.xlsx')
zscore=[]
z2=[]
z3=[]
z4=[]
for i in range(0,6):
    cc=country[i]
    n=int((len(cc.columns)+1)/5)
    zscore=[]
    zwoq=[]
    zwogdp=[]
    
    for ii in range(0,n):
        
        input=cc.iloc[2:,5*ii:5*ii+4]
        input.columns=['Date','PX_LAST','ECO_RELEASE_DT','ACTUAL_RELEASE']
        
        update,m=update_release_dates(input,transformation=transf.iloc[ii,i])
        z=usalphaIC(release=update['Diff1M'])
        zscore.append(z)
        if frequency.iloc[ii,i]!='Q':
            zwoq.append(z)
        if sector.iloc[ii,i]!='GDP':
            zwogdp.append(z)

    us_z=pd.concat(zscore,axis=1)
    us_z=pd.DataFrame(us_z.mean(axis=1))
    z2.append(us_z)
    
    zwoq_z=pd.DataFrame(pd.concat(zwoq,axis=1).mean(axis=1))
    z3.append(zwoq_z)
    zwogdp_z=pd.DataFrame(pd.concat(zwogdp,axis=1).mean(axis=1))
    z4.append(zwogdp_z)
    


# In[ ]:

frequency=pd.read_excel('fxfrequency.xlsx')
sector=pd.read_excel('fxsector.xlsx')
z3=[]
z4=[]
for i in range(0,9):
    cc=country[i]
    n=int((len(cc.columns)+1)/5)
    zwoq=[]
    zwogdp=[]
    for ii in range(0,n):
        
        
        if frequency.iloc[ii,i]!='Q':
            zwoq.append(z2[i][ii])
        if sector.iloc[ii,i]!='GDP':
            zwogdp.append(z2[i][ii])
            

    zwoq_z=pd.DataFrame(pd.concat(zwoq,axis=1).mean(axis=1))
    z3.append(zwoq_z)
    zwogdp_z=pd.DataFrame(pd.concat(zwogdp,axis=1).mean(axis=1))
    z4.append(zwogdp_z)
total_woq=pd.concat(z3,axis=1)
total_wogdp=pd.concat(z4,axis=1)


# In[30]:

#fieldss= ['AUD', 'CAD', 'CHF', 'EUR', 'GBP', 'JPY', 'NOK', 'NZD', 'SEK']
fields_rates= ['AUD', 'CAD', 'EUR', 'GBP', 'JPY', 'USD']
country_rates=[au,canada,eu,uk,japan,us]
#country=[au,canada,switz,eu,uk,japan,norway,nz,sweden]
transf=pd.read_excel('g10_G_transformation.xlsx')
transf_rates=transf[fields_rates]    
#fieldss= [['AUD_3Y','AUD_10Y'], 'CAD_10Y', ['GER_2Y','GER_5Y','GER_10Y','GER_30Y','FRA_10Y'], ['GBP_2Y','GBP_10Y'], 'JPY_10Y', ['USD_2Y','USD_5Y','USD_10Y','USD_20Y','USD_30Y']]
#country=[au,canada,eu,uk,japan,us]
zscore=[]

z=[]
z2=[]
input_data={}
title=list(au.iloc[0,:].index)
jp_title=[]
for i in range(0,int(len(title)/5)+1):
    a=title[i*5]
    jp_title.append(a)
    
for i in range(0,6):
    i=0
    au_data={}
    cc=country_rates[i]
    n=int((len(cc.columns)+1)/5)
    zscore=[]
    input=[]
   
    for ii in range(0,n):
     
        input=cc.iloc[2:,5*ii:5*ii+4]
         
        input=cc.iloc[2:,5*ii:5*ii+4]
        input.columns=['Date','PX_LAST','ECO_RELEASE_DT','ACTUAL_RELEASE']
        zscore.append(input)
for i in range(0,n):
    au_data[jp_title[i]]=zscore[i]
     


# In[31]:

au_data


# In[ ]:

update,m=update_release_dates(au_data,transformation=transf.iloc[ii,i])
        z=usalphaIC(release=update['Diff1M'],sign=1,date_range=dt_range_rates)
#        ic.append(result)
#        mt.append(m)
        zscore.append(z)

#    au_z=zscore    

    z2.append(zscore)

z=[]
for i in range(0,6):
    z2_m=pd.concat(z2[i],axis=1).mean(axis=1)
    z.append(z2_m)
total_m=pd.concat(z,axis=1)


# In[32]:

update,m=update_release_dates(au_data,transformation=transf.iloc[ii,i])


# In[36]:

au_data[1]


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




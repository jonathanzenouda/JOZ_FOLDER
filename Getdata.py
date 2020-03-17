#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import swat
import logging
import time
import getpass
import requests
import pandas as pd
import datetime as dt

from requests.exceptions import HTTPError


# In[ ]:


host = 'https://newviyawaves.sas.com/cas-shared-default-http/'
username = 'frascb'
password = 'XXXXX' 

caslib = 'casuser'
table_name = 'covid19'

# check new file and update every x seconds
duration = 60*60*5 

baseurl = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'


# In[ ]:


# Connect to CAS server
s = swat.CAS(host, port, username, password)


# In[ ]:


while True:
    
    update = False
    date = dt.datetime.today()

    while update == False:
        try:
            url = baseurl + str(date.strftime('%m-%d-%Y')) + '.csv'
            r = requests.get(url)
            logging.info('Try: ',url)
            r.raise_for_status()
        except HTTPError:
            date = date + dt.timedelta(days=-1)
        else:
            df = pd.read_csv(url, error_bad_lines=False)
            logging.info('Download done:', url)
            if s.tableExists(name=table_name,caslib=caslib).exists:
                s.dropTable(name=table_name,caslib=caslib)
            s.upload_frame(df, importoptions=None, casout={'caslib':caslib, 'name':table_name,'promote':True})
            logging.info('Table updated on server:', url)
            update = True
            
    time.sleep(duration)


# In[ ]:





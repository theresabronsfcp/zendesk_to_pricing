import requests
import json
import pandas as pd
import time
from datetime import datetime
import sys
import yaml
try:
    sys.path.insert(1, '/home/administrator/Desktop/python/Modules')
    import Mod_ImportData as Im
except:
    sys.path.insert(1, '/home/theresa/Desktop/github_stuff/Modules')
    import Mod_ImportData as Im
engine=Im.pricing_rw()

with open('/home/administrator/creds/config.yml') as f:
    data=yaml.load(f, Loader = yaml.FullLoader)
data=data['default']

real_credentials = data['Zendesk_Live']['username'],data['Zendesk_Live']['token']

#find the stamp of the most recent addition to the table.
start_time_query = '''select max(stamp) as start_time from zendesk_customer_calls;'''
#query should return a string time like '%Y-%m-%d %H:%M:%S'


#I think the calls that are not in the table will have creation time after this timestamp,
#so we will use it as a lower bound for the incremental call stats request.
#query should return a string time like '%Y-%m-%d %H:%M:%S'
start_time = pd.read_sql(start_time_query, con=engine)['start_time'][0]

print(start_time)

start_time = datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S')
start_time_unix = str(datetime.strftime(start_time, '%s'))


def get_1k_calls(start_time_unix='', next_page = ''):
    #not sure what it should return, but new start_time in the 
    #form of calls.content['end_time'] should be one thing
    print('nextpageis', next_page, 'thatwasit')
    if next_page == '' and start_time_unix !='':
        get_calls = 'https://fcpeuro.zendesk.com/api/v2/channels/voice/stats/incremental/calls?start_time='
        get_calls += start_time_unix
    elif next_page == '' and start_time_unix == '':
        print('both empty')
        return 'No more calls right now.'
    else:
        
        #in this case: next_page != '', doesn't matter what start_time_unix is

        get_calls = next_page
        
    calls=requests.get(get_calls, auth=real_credentials)

    calls_json = json.loads(str(calls.content.decode()))
    
    next_page = calls_json['next_page']
    
    calls_list = calls_json['calls']
    
    calls_customers =[]
    
    for call in calls_list:
        if call['ticket_id'] != None:
            calls_customers.append(  
                {'call_id':call['id'], 'ticket_id':str(call['ticket_id']) })
       
    df = pd.DataFrame(calls_customers)
    #if there is nothing in the dataframe, I'm going to exit the script because there's nothing to pull from ZD.
    #if len(df) == 0:
        #{'next_page':next_page, 'data': df }
        
    print(next_page, len(df))
    return {'next_page':next_page, 'data':df }



next_page = ''
i=0

return_val=get_1k_calls(start_time_unix=start_time_unix, next_page = next_page)
#input these to table.

while len(return_val['data']) > 0 :
    print('pass ', str(i))
    #insert what has already been collected into the table
    try:
        #insert contents of df into pricing.zendesk_customer_calls
        print('inputting to table')
        df = return_val['data']
        df.to_sql('zendesk_customer_calls', 
                  con = engine, if_exists = 'append', chunksize = 1000, index=False)
    except Exception as e:

        print('Failed to input calls begining at ', start_time)
        print(e)
        exit()
        
    i += 1
    start_time_unix = ''
    next_page = return_val['next_page']
    print(return_val['data'])
    return_val=get_1k_calls(start_time_unix=start_time_unix, next_page = next_page)
    



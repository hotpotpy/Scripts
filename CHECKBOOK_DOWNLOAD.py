# -*- coding: utf-8 -*-
"""Created on Thu Feb 22 10:40:37 2018
@author: Aaron
"""
from bs4 import BeautifulSoup
import requests
import pandas as pd
import xmltodict

#%%SETUP THE CONSTANT VARIABLES FIRST
#Vendor code list: https://www.checkbooknyc.com/ref_code_list/vendor 

#Set the variables
#Path to save the files
path = "c:\\users\\aaron\\checkbook\\BUDGET\\"

#Type of data must be 'Contracts','Budget','Revenue','Payroll','Spending','Spending_OGE', or 'Contracts_OGE'
type_of_data = 'Budget'

#Modify the key word arguments to be used in the xml_checkbook_input function to specify criteria

#The name:value must follow the structure of checkbook API
#https://www.checkbooknyc.com/data-feeds/api

#Example 1 (when type_of_data=Contracts): kwargs = {'status':'active','category':'all'}
#Example 2 (when type_of_data=Spending): kwargs = {'fiscal_year': 2017}

#The xml_checkbook_input function will convert the kwargs dictionary to keyword parameters
#Example: kwargs = {'status':active','category:'all'}
#         type_of_data = 'Contracts'
#xml_checkbook_input(type_of_data,1,5,**kwargs) -> xml_checkbook_input('Contracts',1,5,status='active',category='all')

kwargs = {}

agencies_to_search = ['040']

#The list of agency codes can be downloaded from
#https://www.checkbooknyc.com/ref_code_list/agency 

#040:DOE has many records so if collecting for all agencies, do this last or skip this

#Split agency list to two sections
#agencies_to_search = ['004','008','009','010','011','012','013','014','017','019','021','025','037','038','039',
#                      '042','057','067','068','069','071','095','096','101','102','103','125']

#%%
def xml_checkbook_input(type_of_data, record_counter, max_records, **kwargs):
    """ str,str,str,dict -> str 
    Will setup the xml text to be used for requests.
    More arguments will filter or narrow down the results, see above."""
    xml = """
            <request>
                <type_of_data>""" + str(type_of_data) + """</type_of_data>
                <records_from>""" + str(record_counter) + """</records_from>
                <max_records>""" + str(max_records) + """</max_records>
                <search_criteria>
        """
    #Add in each extra argument to the xml string
    for key, value in kwargs.items():
        xml += """
                    <criteria>
                            <name>"""+str(key)+"""</name>
                            <type>value</type>
                            <value>""" + str(value) + """</value>
                    </criteria>
                    """
    xml += '</search_criteria></request>'
    return xml
     
def checkbook_request(xml):
    """str -> request.models.Response
    Use requests module to get checkbook nyc data"""
    head = {'Content-Type': 'text/xml'}
    r = requests.post('https://www.checkbooknyc.com/api',data=xml, headers=head) 
    return r

#%%

#Remove duplicates from list [I was too lazy to remove them from above]
agencies_to_search = sorted(list(set(agencies_to_search)))

#dic_list stores list of dictionaries, the dic dictionary represents one transaction/one row of data
#dic processes one record, puts a copy to dic_list, then becomes empty again for the next record
#record_list keeps track of the number of records for each agency

dic_list = []
dic = {}
record_list = []

#%%

#Loop through each agency
for agency in agencies_to_search:
    dic_list = []
    #Do initial search of only first 5 results of agency to see if there are any records
    #If there are 0 records, then go to next agency
    xml = xml_checkbook_input(type_of_data,1,5,**kwargs)
    r = checkbook_request(xml)
    soup = BeautifulSoup(r.text,"lxml")
    transactions = soup.find_all('transaction')
    tags = transactions[0].find_all()
    
    records = soup.find('record_count').text
    log = [key for key in kwargs]
    record_list.append([type_of_data, records] + log)
    if records == '0':
        print("NO RECORDS FOR " + str(agency))
        continue
    else:
        #Will calculate how many times to loop (results is max 1000 results)
        #Ex: numloops is 4300, round(4300/1000) is 4, but looping (0,4) below gives 1,1001,2001,3001
        #So +1 is needed to the loop becomes 1,1001,2001,3001,4001 for the last few hundred
        numloops = round(int(records)/1000)+1
    for i in range(0,numloops):
        dic_list = []
        #Get xml ready according to the counter and agency, request it, use BeautifulSoup to process
        xml = xml_checkbook_input(type_of_data,i*1000+1, 1000,**kwargs)
        r = checkbook_request(xml)
        soup = BeautifulSoup(r.text,"lxml")
        #Get all transactions
        transactions = soup.find_all('transaction')
    
        #For each transaction, clear the dic, then read in all the tags and append that dic to dic_list
        for t in transactions:
            dic.clear()    
            tags = xmltodict.parse(str(t))
            for key in tags['transaction']:
                dic[key] = tags['transaction'][key]
#            for tag in tags:
#                try:
#                    dic[tag] = t.find(tag.lower()).text
#                except:
#                    dic[tag] == None
            dic_list.append(dic.copy())    
        print("ITERATION %s DONE OUT OF %s." % (str(i),str(numloops)))
        df = pd.DataFrame.from_records(dic_list)
        df['TYPE_OF_DATA'] = type_of_data
        for key, value in kwargs:
            df[key] = value   
        df.to_csv(path + type_of_data + "_" + str(agency) + "_ITERATION_" + str(i) + ".csv",index=False)
        del df
    print("Finished agency " + str(agency) + " with " + str(records) + " results.")
   # time.sleep(random.uniform(1,2))
#df = pd.DataFrame.from_records(dic_list)
rec = pd.DataFrame(record_list)
import pandas as pd
import sqlite3
import numpy as np
import csv
import numpy as np
import datetime

#Handle rows with more columns than expected and append them to a list to treat them separately
line     = []
expected = []
saw      = []
cont     = True
list=[]

while cont == True:
    try:
        data = pd.read_csv('reddit_exercise_data.csv',skiprows=line)
        cont = False
    except Exception as e:
        errortype = e.message.split('.')[0].strip()
        if errortype == 'Error tokenizing data':
           cerror      = e.message.split(':')[1].strip().replace(',','')
           nums        = [n for n in cerror.split(' ') if str.isdigit(n)]
           expected.append(int(nums[0]))
           saw.append(int(nums[2]))
           line.append(int(nums[1])-1)

if line != []:
    list= list.append(line)


#Delete empty rows
data=data.dropna()


#Rename columns from the csv doc to dataframe so they match with the ones in the db
#Create empty bucket columns
data.rename(columns={'app_bought':'apps_bought', 'money_spent;;':'money_spent'}, inplace=True)
data['apps_bought_bucket']= ""
data['money_spent_bucket']= ""


#Fix bug (values on money_spent were followed by ;;)
new=[]
for cell in data['money_spent']:
    cl=len(cell)-2
    cell=cell[:cl]
    new.append(cell)

data['money_spent']=new

#Convert to a numeric format so we can analyse them (to create the buckets)
data['money_spent']=pd.to_numeric(data['money_spent'])
data['apps_bought']=pd.to_numeric(data['apps_bought'])


#Create and choose buckets (4 intervals delimited by percentiles)
msp= data['money_spent'].describe()
mean1=msp[1]            #Mean value of money_spent
min1=msp[3]             #Min value of money_spent
money25=msp[4]          #Percentile 25%  of money_spent
money50=msp[5]          #Percentile 50%  of money_spent==median
money75=msp[6]           #Percentile 75% of money_spent
max1=msp[7]              #Max value of money_spent

col1=[]
for row in data['money_spent']:
    if row<money25 or row==money25:
        col1.append('Money_Low')
    elif row>money25 and row<money50:
        col1.append('Money_MedLow')
    elif row>money50 or row==money50 and row<money75:
        col1.append('Money_MedHigh')
    else:
        col1.append('Money_High')

data['money_spent_bucket']=col1



ab=data['apps_bought'].describe()
col2=[]
for row in data['apps_bought']:
    if row < ab[4] or row==ab[4]:
        col2.append('Apps_Low')
    elif row>ab[4] and row<ab[5]:
        col2.append('Apps_MedLow')
    elif row>ab[5] or row==ab[5] and row<ab[6]:
        col2.append('Apps_MedHigh')
    else:
        col2.append('Apps_High')

data['apps_bought_bucket']= col2


#DataFrame to SQLite DB
db=sqlite3.connect('exercise_database.db')
c=db.cursor()
db.text_factory = str

#Create index and product_name columns (can be removed if necessary but just in case we also need this info)
c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
            .format(tn='reviews', cn='index', ct='numeric'))
c.execute("ALTER TABLE {tn} ADD COLUMN '{cn}' {ct}"\
           .format(tn='reviews', cn='product_name', ct='text'))


data.to_sql('reviews', db, if_exists='append', chunksize=1000)
data.to_csv('newcsv.csv')


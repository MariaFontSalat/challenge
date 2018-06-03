import sqlite3
import csv


db=sqlite3.connect('exercise_database.db')
c=db.cursor()

#Query1
q="select iso, AVG(score) from reviews group by iso"
c.execute(q)
rows=c.fetchall()

#Query2
q2="select apps_bought_bucket, MAX(score) from reviews group by apps_bought_bucket"
c.execute(q2)
rows2=c.fetchall()

#Query3
q3="select strftime('%Y/%m/%d', date), AVG(score) from reviews group by strftime('%Y/%m/%d', date) order by date asc"
c.execute(q3)
rows3=c.fetchall()


#Export to csv
fp = open('queriesresults.csv', 'w')
myFile = csv.writer(fp)
myFile.writerows(rows)
myFile.writerows(rows2)
myFile.writerows(rows3)
fp.close()
import sys
import datetime
import os.path, time


#MySQL
import mysql.connector



#MSSQL
#https://code.google.com/p/pyodbc/downloads/detail?name=pyodbc-3.0.7.win32-py2.7.exe&can=2&q=
import pyodbc

server = 'SYS01'
server = 'GARUDA63'
server = 'MYSQL'
boolSQL = True
booldebug = True
cursor = ""

'''
    Name: log
    Param: messaggio da stampare
    Descr: funzione che scrive su file di log il messaggio desisderato
'''

logfile = open('log.log','a')
def log(msg):
	msgfull = str(datetime.datetime.now()) + "," + msg+"\n"
	logfile.write(msgfull)

def insertVersione(filename):
	'''
	'''
	rs = cursor.execute("SELECT MAX(versione) AS versione FROM GPUVersione").fetchone()
	try:
		field_versione = rs.versione + 1
	except:
		field_versione = 1
	field_data = time.ctime(os.path.getmtime(filename))
	query = "INSERT INTO GPUVersione (versione, dataexport) VALUES ("+str(field_versione)+",'"+str(field_data)+"')"

	if (booldebug):
		log(query)
		
	if (boolSQL):
		cursor.execute(query)
	
	return(field_versione)
	
	
def insertTable(filename, name, versione):
	'''
	'''
	lunghezza = 0

	cursor.execute("DELETE FROM "+name)
	
	with open(filename, 'r') as f:
		first_line = f.readline()
		lunghezza = len(first_line.split(";"))
		print(lunghezza)
        
	parziale = "INSERT INTO "+name+" ("
    
	for i in range (0,lunghezza):
		parziale+=("`Column "+str(i)+"`,")
        
	parziale = parziale[:-1]
	parziale+=") VALUES ("

	with open(filename, 'r') as f:
		for line in f:
			query = parziale
			line = line.replace('"','').replace('\'','').replace('\n','').split(";") 
			for i in range (0,lunghezza):
				query+=("\'"+line[i]+"\',")
			query = query[:-1]
			query+=','+str(versione)
			query+=")"

			if (booldebug):
				log(query)
            
			if (boolSQL):
				cursor.execute(query)


if (__name__ == "__main__"):
	'''
	'''
	
	print("Starting: SQL is ", boolSQL )
	if (server == 'MYSQL'):
		#-- Connessione MYSQL
		connection = mysql.connector.connect(user='root',password='',host='localhost',database='orario')	

	if (server == 'SYS01'):
		#-- Connessione MSSQL
		connection = pyodbc.connect('DRIVER={SQL Server};SERVER=edu-sys01\marconi;DATABASE=;UID=;PWD=')

	if (server == 'GARUDA63'):
		#-- Connessione MSSQL
		connection = pyodbc.connect('DRIVER={SQL Server};SERVER=;DATABASE=;UID=;PWD=')

	#-- OPEN CURSOR
	cursor = connection.cursor()
	
	boolFirstfile = True
	
	with open("conf.conf", 'r') as f:
		for line in f:
			line = line.replace('\n','').split(',')
			if (line[0][0] != '#'):
				print (line)
				log("file: "+line[0]+" table: "+line[1])
				if (boolFirstfile == False):
					field_versione = insertVersione(line[0])
					boolFirstfile = True

				field_versione = 1
				insertTable(line[0], line[1], field_versione)
	
	
	#-- C O M M I T !
	try:
		cursor.commit()
	except:
		connection.commit()


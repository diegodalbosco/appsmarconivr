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
	try:
		cursor.execute("SELECT MAX(versione) AS versione FROM GPUVersione")
		rs = cursor.fetchone()
		field_versione = rs[0] + 1
	except:
		field_versione = 1

	field_data = time.ctime(os.path.getmtime(filename))
	query = "INSERT INTO GPUVersione (versione, dataexport) VALUES ("+str(field_versione)+",'"+str(field_data)+"')"

	if (booldebug):
		log(query)
		
	if (boolSQL):
		cursor.execute(query)
	
	return field_versione
	
	
def insertTable(filename, name, versione):
	'''

	'''
	lunghezza = 0

	#cursor.execute("DELETE FROM "+name)
	
	with open(filename, 'r') as f:
		first_line = f.readline()
		lunghezza = len(first_line.split(";"))
		print(lunghezza)
        
	parziale = "INSERT INTO "+name+" ("
    
	for i in range (0,lunghezza):
		parziale+=("`Column "+str(i)+"`,")
        
	parziale = parziale[:-1]
	parziale+=",versione) VALUES ("

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
		connection = mysql.connector.connect(user='root',password='',host='',database='')	
		#connection = pyodbc.connect("DRIVER={MySQL ODBC 3.51 Driver}; SERVER=localhost;DATABASE=orario; UID=root; PASSWORD=root$$2014;")

	if (server == 'SYS01'):
		#-- Connessione MSSQL
		connection = pyodbc.connect('DRIVER={SQL Server};SERVER=;DATABASE=;UID=;PWD=')

	if (server == 'GARUDA63'):
		#-- Connessione MSSQL
		connection = pyodbc.connect('DRIVER={SQL Server};SERVER=;DATABASE=;UID=sigma;PWD=')

	#-- OPEN CURSOR
	cursor = connection.cursor(buffered=True)
	
	boolFirstfile = False
	
	with open("conf.conf", 'r') as f:
		for line in f:
			line = line.replace('\n','').replace('\r','').split(',')
			if (line[0][0] != '#'):
				print (line)
				log("file: "+line[0]+" table: "+line[1])
				print("file: "+line[0]+" table: "+line[1])
				if (boolFirstfile == False):
					field_versione = insertVersione(line[0])
					boolFirstfile = True

				#field_versione = 1
				insertTable(line[0], line[1], field_versione)
	
	
	#-- C O M M I T !
	try:
		cursor.commit()
	except:
		connection.commit()


import sqlite3

connection = sqlite3.connect('data.db')

cursor = connection.cursor()
tab =  'CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY, username text, password text)'
cursor.execute(tab)

user =(1, 'juned','1122')
insert = "INSERT INTO users VALUES(?,?,?)"
cursor.execute(insert,user)
 
for row in cursor.execute('select * from users'):
    print(row)
connection.commit()
connection.close()
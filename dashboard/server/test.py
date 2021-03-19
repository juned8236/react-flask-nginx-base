from databaseConnection import connect
from werkzeug.security import generate_password_hash

connection = connect()

cursor = connection.cursor()
print(cursor)
tab =  'CREATE TABLE IF NOT EXISTS users(id  SERIAL PRIMARY KEY, username text, password text)'
cursor.execute(tab)
# user =(1, 'juned','1122')
insert = f"INSERT INTO users VALUES(4,'ahmed','1122')"
cursor.execute(insert)
 
# for row in cursor.execute('select * from users'):
#     print(row)
connection.commit()
connection.close()
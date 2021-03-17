import sqlite3
from flask_restful import Resource, reqparse

class User:
    def __init__(self, _id, username, password):
        self.id = _id
        self.username = username
        self.password = password

    @classmethod
    def find_by_username(cls, username):
        connection = sqlite3.connect('data.db')
        cursor = connection.cursor()
        query = "SELECT * FROM users where username=?"
        result  = cursor.execute(query,(username,))
        row = result.fetchone()
        if row:
            # user = User(row[0], row[1], row[2])
            # user = cls(row[0], row[1], row[2])
            user = cls(*row)
        else:
            user = None       
        connection.close()
        return user

    @classmethod
    def find_by_id(cls, _id):
        connection = sqlite3.connect('data.db')
        cursor = connection.cursor()
        query = "SELECT * FROM users where id=?"
        result  = cursor.execute(query,(_id,))
        row = result.fetchone()
        if row:
            user = cls(*row)
        else:
            user = None
        
        connection.close()
        return user
    
    @classmethod
    def find_by_password(cls, password):
        connection = sqlite3.connect('data.db')
        cursor = connection.cursor()

        query = "SELECT * FROM users where password=?"
        result  = cursor.execute(query,(password,))
        row = result.fetchone()
        if row:
            user = cls(*row)
        else:
            user = None
        
        connection.close()
        return user

class UserRegister(Resource):
    parser =  reqparse.RequestParser()
    parser.add_argument('username',
    type = str,
    required = True,
    help = 'This field cannot be blank')
    
    parser.add_argument('password',
    type = str,
    required = True,
    help = 'This field cannot be blank')
    
    def post(self):
        data  = UserRegister.parser.parse_args()
        print(data)
        connection = sqlite3.connect('data.db')
        cursor = connection.cursor()
        query = "Insert into users values(NULL,?,?)"
        cursor.execute(query,(data['username'], data['password'] ) )
        
        connection.commit()
        connection.close()

        return {'message': 'User created successfully. '},201
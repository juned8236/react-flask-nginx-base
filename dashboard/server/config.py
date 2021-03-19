import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import psycopg2
import sys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


host = os.getenv("HOST")
database = os.getenv("DATABASE")
user = os.getenv("ADMIN")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")

basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    TESTING = False
    CSRF_ENABLED = True
    SECRET_KEY = 'xya2342hsdfajhlhi'
    SQLALCHEMY_DATABASE_URI = f"postgresql://{user}:{password}@{host}:{port}/{database}"  

config_setting = {
    'config' : Config
}

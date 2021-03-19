from sqlalchemy import Column, Integer, String
from sqlalchemy.types import Date
from server.config import Base

class Fake(Base):
    __tablename__ = 'Fake'

    id = Column(Integer, primary_key = True, index = True)
    case = Column(Integer)
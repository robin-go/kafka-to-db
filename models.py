import uuid
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

engine = create_engine('mysql+mysqldb://root:111111@localhost/arale-7moor?charset=utf8')
Base = declarative_base()


def generate_uuid():
    return uuid.uuid4().hex


class Record(Base):
    __tablename__ = 'records'

    id = Column(String(32), primary_key=True, default=generate_uuid)
    botid = Column(String(100), nullable=False)
    sessionid = Column(String(100))
    usertype = Column(String(100))
    msgcontent = Column(String(1024))
    msgtype = Column(String(100))
    msgtime = Column(String(100), default=datetime.now)
    flag = Column(Integer, default=0)


if __name__ == '__main__':
    Base.metadata.create_all(engine)

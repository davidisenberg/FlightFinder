#!/usr/bin/env python

from sqlalchemy import *
import pymysql
from sqlalchemy.orm import Session, sessionmaker
from model.direct import Direct
from  sqlalchemy.ext.declarative  import  declarative_base
import pandas as pd

Base  =  declarative_base()

class DirectsDB(Base):
    __tablename__ = 'directs'
    Id = Column(Integer, primary_key=True)
    FlyFrom = Column(String)
    FlyTo = Column(String)

    def __init__(self, fly_from, fly_to):
        self.FlyFrom = fly_from
        self.FlyTo = fly_to


class DirectsRepository:

    def __init__(self):
        self.engine = create_engine("mysql+pymysql://davidisenberg:vpnzv8zy@localhost/ff?host=localhost?port=3306")

    def get_directs(self, fly_from = None):
        Session = sessionmaker(bind=self.engine)
        s = Session()
        #directs = []
        #for row in s.query(DirectsDB).filter(DirectsDB.FlyFrom == fly_from):
        #    directs.append(Direct(row.FlyFrom, row.FlyTo))
        if fly_from is None:
            directs = pd.read_sql(s.query(DirectsDB).statement, s.bind)
        else:
            directs = pd.read_sql(s.query(DirectsDB).filter(DirectsDB.FlyFrom == fly_from).statement, s.bind)

        return directs

    def insert_directs(self, directs):
        Session = sessionmaker(bind=self.engine)
        s = Session()
        directs_dbs = []
        for direct in directs:
            directs_dbs.append( DirectsDB(direct.FlyFrom,direct.FlyTo))

        s.bulk_save_objects(directs_dbs)
        s.commit()


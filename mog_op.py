#!/usr/bin/env python
# -*- coding:utf-8 -*-

import pymongo
import re,os
import logging
from logging import getLogger
logging.basicConfig(level=logging.INFO)
local_logger = getLogger(__name__)
import pandas as pd
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
MONGO_HOST=os.environ.get("MONGO_HOST")
MONGO_USER=os.environ.get("MONGO_USER")
PASSWORD=os.environ.get("PASSWORD")
AUTHSOURCE=os.environ.get("AUTHSOURCE")

class MongoOp(object):
    def __init__(self,db='youtube_comment',logger=None):
        self.con = pymongo.MongoClient(MONGO_HOST,
                                       27017,
                                       username=MONGO_USER,
                                       password=PASSWORD,
                                       authSource=AUTHSOURCE,
                                       authMechanism='SCRAM-SHA-1')
        
        self.db=self.con[db]
        if logger:
            self.logger=logger
        else:
            self.logger=local_logger

    def __del__(self):
        if self.con:
            self.con.close()
            self.con=None
    def close(self):
        if self.con:
            self.con.close()
            self.con=None
    def get_col(self,col):
        self.logger.info(f"col={col}")
        return self.db[col]
    

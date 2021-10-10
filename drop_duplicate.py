import pandas as pd
import numpy as np
import gc
import logging

from config.database import HOST, PORT, USER, PASSWORD, DATABASE

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import types as sqltype
from config.database import HOST, PORT, USER, PASSWORD, DATABASE

psql_engine = create_engine("postgresql://"+USER+":"+PASSWORD+"@"+HOST+":"+str(PORT)+"/"+DATABASE)

with psql_engine.connect() as conn:
    df = pd.read_sql("SELECT thread_id FROM thread WHERE thread_type='email';", con= conn)

with psql_engine.connect() as conn:
    count_num = 0
    for thisid in df['thread_id'].values :
        sql = """ DELETE FROM thread WHERE thread_id='%s'""" %(thisid)
        conn.execute(sql)
        count_num+= 1
        if not(count_num%1000) :
            print("Delete:", count_num)

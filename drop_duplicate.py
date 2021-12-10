import pandas as pd
import numpy as np
import gc
import logging
from tqdm import tqdm
from config.database import HOST, PORT, USER, PASSWORD, DATABASE

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import types as sqltype
from config.database import HOST, PORT, USER, PASSWORD, DATABASE

psql_engine = create_engine("postgresql://"+USER+":"+PASSWORD+"@"+HOST+":"+str(PORT)+"/"+DATABASE)

def escape(s):
    return (s).replace("'", "''").replace('%', '%%').replace('<','').replace('>','')
    
with psql_engine.connect() as conn:
    df_aliase = pd.read_sql("SELECT * FROM aliase WHERE source IS NULL;", con= conn)

with psql_engine.connect() as conn:
    #count_num = 0
    for thisid in tqdm(df_aliase['aliase_id'].values) :
        sql = """ DELETE FROM aliase WHERE aliase_id='%s'""" %(escape(thisid))
        conn.execute(sql)
        #count_num+= 1
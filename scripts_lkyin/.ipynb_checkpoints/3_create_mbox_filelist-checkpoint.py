# -*- coding: utf-8 -*-
"""
Created on Thu Aug 09 17:03:06 2018

@author: zzy
"""

import os,pg8000

MBOX_DIR="./mbox"
conn = pg8000.connect(host="localhost",user="postgres",password="password",database="postgres")
cursor=conn.cursor()
cursor.execute("select pj_alias from lists where dev_is_available is TRUE")
PJnameIn=cursor.fetchall()
PJnames=[]
for x in PJnameIn:
    PJnames.append(x[0])
conn.close()

filelist_in=os.listdir(MBOX_DIR)

list_out=open("mbox_filelist.txt","w+")
for element in filelist_in:
    if "commits-" in element or "dev-" in element:
        pjname=element.split("-")[0]
        if pjname in PJnames:
            list_out.write(MBOX_DIR+"/"+element+"\n")
            
list_out.close()
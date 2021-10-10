# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import urllib2
import urllib
import codecs
import unicodedata
import sys
import os
import pg8000
import time,datetime

from bs4 import BeautifulSoup

html_date='2019.3'

DateHlimit=201903
DateLlimit=190001


Date_start=201903 #//at least 6 months from DateHlimit
TABLE_POSTFIX='_2019_3'

STATUS=['incubating','graduated','retired']



Name2id_set={
'zetacomponents':'zeta',
'hadoopdevelopmenttools(hdt)':'hdt',
'climatemodeldiagnosticanalyzer':'cmda',
'openoffice.org':'openoffice',
'openofficeorg':'openoffice',
'openclimateworkbench':'climate',
'empire-db':'empire',
'beanvalidation':'bval',
'amber':'oltu'
}

baseurl='http://mail-archives.apache.org/mod_mbox/'

db_config=open('./config/db_config')
dbHOST=db_config.readline().split('\"')[1]
dbUSER=db_config.readline().split('\"')[1]
dbPASS=db_config.readline().split('\"')[1]
dbDB=db_config.readline().split('\"')[1]
db_config.close()

def sort(a):
    for k in range(len(a)):
        (a[k][0],a[k][1]) = (a[k][1],a[k][0])
    a.sort()
    for k in range(len(a)):
        (a[k][0],a[k][1]) = (a[k][1],a[k][0])

def read2soup(url):
    req=urllib2.Request(baseurl+url)
    con=urllib2.urlopen(req)
    doc=con.read()
    con.close()
    soup = BeautifulSoup(doc,"html.parser")
    return soup

def Schedule(a,b,c):

    per = 100.0 * a * b / c
    if per > 100 :
        per = 100
    print '%.2f%%' % per
    
def check_pj_id(name_in):
    if name_in in Name2id_set:
        return Name2id_set[name_in]
    return name_in


# Converts text into ascii to fix weird characters
def asciiCodify(s):
    try:
        u = unicode(s, errors="replace")
        v = u.encode("ascii", "replace")
        return v
    except:
        # s is empty
        return "EMPTY"
# Fix any characters that might mess up script
def escape(s):
    return unicode(s.replace("'", "''").replace('%', '%%'))


soup_attic = BeautifulSoup(open('source_html/Apache Attic('+html_date+').html'),"html.parser") 
temp_contentin=soup_attic.html.body.findAll('div')[19]
temp_list=temp_contentin.findAll('li')
attic_list=[]#graduated project but now in attic
for i in range(len(temp_list)):
    attic_list.append(str(temp_list[i]).split('.html')[0].split('/')[-1])
 
startt=time.time()
PJ_list_info=[]
PJ_alias_list=[]

soup1 = BeautifulSoup(open('source_html/Apache Projects List('+html_date+').html'),"html.parser") 
temp_listin=soup1.html.body.findAll('tr')
PJstatus=-1
for i in range(len(temp_listin)):
    if str(temp_listin[i].find('th'))=='<th>Project</th>':
        PJstatus+=1
        continue
    PJname=temp_listin[i].find('a').text
    PJid=check_pj_id(str(temp_listin[i]).split('"')[1])
    PJsponsor=temp_listin[i].findAll('td')[2].text
    PJintro=temp_listin[i].findAll('td')[1].text
    PJstartdate=datetime.datetime.strptime(temp_listin[i].findAll('td')[4].text,"%Y-%m-%d").date()
    PJenddate='NULL'
    PJ_url='http://incubator.apache.org'+(str(temp_listin[i].find('a')['href']))
    if PJstatus>0:
        PJenddate=datetime.datetime.strptime(temp_listin[i].findAll('td')[5].text,"%Y-%m-%d").date()
    in_attic=False
    if PJid in attic_list:
        in_attic=True
    #if check_pj_id()
    PJ_list_info.append([PJid,PJname,PJstatus,PJsponsor,PJstartdate,PJenddate,0,0,in_attic,PJintro,PJ_url])
    #0 for this project is not available,one for has dev,one for has commit
    PJ_alias_list.append(PJid)
    
endt=time.time()    
print endt-startt

soup2 = BeautifulSoup(open('source_html/Available Mailing Lists('+html_date+').html'),"html.parser")    

PJ_name=soup2.html.body.findAll('h3')
PJ_listin=soup2.html.body.findAll('li')

k=0
PJlist=[]

for i in range (0,len(PJ_name)):#get available list name from mailbox
    
    temp=PJ_listin[k].findAll('a')
        
    PJlist.append(temp)

    k=k+len(temp)

sql="select pj_alias from lists"+TABLE_POSTFIX
conn = pg8000.connect(host=dbHOST,user=dbUSER,password=dbPASS,database=dbDB)
read_lists=conn.cursor()
read_lists.execute(sql)
exist_lists_in=read_lists.fetchall()
conn.close()

exist_lists=[]
for i in range(len(exist_lists_in)):
    exist_lists.append(exist_lists_in[i][0])


DLcount=0
DLcount2=1
DLcount3=0
flag=0

pathlist=[]

url_write2file=open("Download_url_list.txt","w+")
url_write2file.close()

for i in range(int(DLcount),len(PJlist)):
#for i in range(123,124):

    url_list=[]
    write2file_flag=0
    for j in range(int(DLcount2),len(PJlist[i])):
        title=PJlist[i][j]["href"]
        pjname=title.split('-')[-2]
        
        try:
            pjindex=PJ_alias_list.index(pjname)#if mbox and list are match
        except:
            continue
        
        if '-dev' in title or '-commits' in title or '-issues' in title or '-notifications' in title:
            try:
                
                #pjindex=PJ_alias_list.index(pjname)
                #if pjname!='accumulo':
                #		continue
                if PJ_list_info[pjindex][2]>0:
                    datehighlimit=int(PJ_list_info[pjindex][5].strftime("%Y%m"))
                else:
                    datehighlimit=DateHlimit
                datelowlimit=int(PJ_list_info[pjindex][4].strftime("%Y%m"))
                soupin=read2soup(PJlist[i][j]["href"])
                listin=soupin.html.body.findAll('span')
                
                
                #listMonth=[]
                for s in range(int(DLcount3),len(listin)):
                    tempid=listin[s]['id']
                    if int(tempid)>=datelowlimit:
                        if int(tempid)<=datehighlimit:
                            #listMonth.append(tempid)
                            #fileOut.write(DLcount)
                            url=baseurl+PJlist[i][j]["href"]+tempid+".mbox"
                            #path="./"+dirName+"/"+PJlist[i][j].string+"-"+tempid+".mbox"
                            
                            url_list.append(url)
                            if PJ_list_info[pjindex][6]==0 and '-dev' in title:
                                PJ_list_info[pjindex][6]=1
                            if PJ_list_info[pjindex][7]==0 and '-commits' in title:
                                PJ_list_info[pjindex][7]=1
                            if PJ_list_info[pjindex][6]==1 and PJ_list_info[pjindex][7]==1:
                                write2file_flag=1
                        else:
                            continue
                    else:
                        break
                    #print str(len(url_list))+','+title
            except:
                continue
                    
    if write2file_flag==1 and int(PJ_list_info[i][4].strftime("%Y%m"))<=Date_start:
        url_write2file=open("Download_url_list.txt","a")

        for x in range(len(url_list)):
            url_write2file.write(url_list[x]+"\n")
            #print url_list[x]
        url_write2file.close()
        print pjname+"      \t"+str(STATUS[PJ_list_info[pjindex][2]])+"\tNo. of mboxes:"+str(len(url_list))
            
    DLcount2=1
    DLcount3=0

incubator_count=0
graduated_count=0
retired_count=0
flist=open("PJ_list.txt","w+")
for i in range(len(PJ_list_info)):
    if PJ_list_info[i][6]+PJ_list_info[i][7]==2 and int(PJ_list_info[i][4].strftime("%Y%m"))<=Date_start:
        print PJ_list_info[i][0]+" "+str(PJ_list_info[i][2])
        flist.write(PJ_list_info[i][0]+" "+str(PJ_list_info[i][2])+"\n")
        if PJ_list_info[i][2]==0:
            incubator_count+=1
        elif PJ_list_info[i][2]==1:
            graduated_count+=1
        elif PJ_list_info[i][2]==2:
            retired_count+=1
flist.close()

print "incubator: "+str(incubator_count)
print "graduated: "+str(graduated_count)
print "retired: "+str(retired_count)



print "writing to database!"

fout=codecs.open("list_db_insert.sql","w+",'utf-8')
conn = pg8000.connect(host=dbHOST,user=dbUSER,password=dbPASS,database=dbDB)
insert_lists=conn.cursor()
sql="select max(listid) from lists"+TABLE_POSTFIX
insert_lists.execute(sql)
pjid=insert_lists.fetchall()[0][0]+1
for i in range(len(PJ_list_info)):
    dev_is_available='False'
    if PJ_list_info[i][0] in exist_lists:
        continue
    if PJ_list_info[i][6]+PJ_list_info[i][7]==2 and int(PJ_list_info[i][4].strftime("%Y%m"))<=Date_start:
        dev_is_available='True'
    if PJ_list_info[i][2]>0:
        pj_url='http://'+PJ_list_info[i][1]+'.apache.org/'
        pj_github_url='https://github.com/apache/'+PJ_list_info[i][1]
        
        sql="insert into lists"+TABLE_POSTFIX+"(listname,status,start_date,end_date,dev_is_available,is_in_attic,intro,sponsor,pj_alias,listid,pj_url,pj_github_url) values "+\
             "('"+str(PJ_list_info[i][1])+"','"+str(PJ_list_info[i][2])+"','"+str(PJ_list_info[i][4])+"','"+str(PJ_list_info[i][5])+"','"+dev_is_available+"','"+str(PJ_list_info[i][8])+\
             "','"+escape(PJ_list_info[i][9])+"','"+escape(PJ_list_info[i][3])+"','"+str(PJ_list_info[i][0])+"','"+str(pjid)+"','"+str(PJ_list_info[i][10])+"','"+str(pj_github_url)+"')"
    else:
        pj_github_url='https://github.com/apache/incubator-'+PJ_list_info[i][1]
        sql="insert into lists"+TABLE_POSTFIX+"(listname,status,start_date,dev_is_available,is_in_attic,intro,sponsor,pj_alias,listid,pj_url,pj_github_url) values "+\
        "('"+str(PJ_list_info[i][1])+"','"+str(PJ_list_info[i][2])+"','"+str(PJ_list_info[i][4])+"','"+dev_is_available+"','"+str(PJ_list_info[i][8])+"','"+escape(PJ_list_info[i][9])+\
        "','"+escape(PJ_list_info[i][3])+"','"+str(PJ_list_info[i][0])+"','"+str(pjid)+"','"+str(PJ_list_info[i][10])+"','"+str(pj_github_url)+"')"
    try:
        insert_lists.execute(sql)
        fout.write(sql+";\n")
        conn.commit()
    except:
        print "push error \n"
        continue
    pjid+=1

        

conn.close()
fout.close()
    
    
    
    
    
    
    

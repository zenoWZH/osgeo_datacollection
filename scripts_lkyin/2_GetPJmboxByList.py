import urllib2
import urllib
import codecs
import unicodedata
import sys
import os

from bs4 import BeautifulSoup

DateHlimit=201908
DateLlimit=190001
baseurl='http://mail-archives.apache.org/mod_mbox/'
mbox_dir="/data/Apachembox/mbox"
cursor_dir="./cursor"
DOWNLOAD_URLS='Download_url_list_latest.txt'

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

def url2path(rdir,url_in):
    url_sections=url_in.split('/')
    date_postfix=url_sections[-1]
    url_name_type=url_sections[-2].split('-')
    mbox_type=url_name_type[-1]
    pjname=url_name_type[-2]
    is_incubator=''
    if len(url_name_type)>2:
        is_incubator="-"+url_name_type[-3]
    path=rdir+"/"+pjname+is_incubator+"-"+mbox_type+"-"+date_postfix
    return path
#read url list
f_url_list=open(DOWNLOAD_URLS,'r')
url_list=f_url_list.read().splitlines() #so no \n at the end of each element
f_url_list.close()

#convert url to path
path_list=[0 for i in range(len(url_list))]
if not os.path.exists(mbox_dir):
    os.mkdir(mbox_dir)
for i in range(len(url_list)):
    path_list[i]=url2path(mbox_dir,url_list[i])

#download mbox
file_count=len(url_list)

if os.path.exists("mbox_filelist.txt"):
    file_path_list=open("mbox_filelist.txt","a")
else:
    file_path_list=open("mbox_filelist.txt","w+")
for i in range(file_count):
    try:
        if not os.path.isfile(path_list[i]):
            urllib.urlretrieve(url_list[i],path_list[i])
            file_path_list.write(path_list[i]+"\n")
    except:
        print "Download Error"
        fileFailed=open('failed.txt','a')
        fileFailed.write(url_list[i]+"\n")
        fileFailed.close()
    print str(i+1)+"/"+str(file_count)

file_path_list.close()
download_done=open("download_done.done","w+")
download_done.close()
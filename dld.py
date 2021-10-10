import urllib.request
import re
import os


proj = "gvsig-desktop-devel"
url = "https://lists.osgeo.org/pipermail/"+proj+"/"

pattern = '([0-9]+\-[\S]+\.txt\.gz)'
 
# pull request
headers = {'User-Agent', 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'}
#opener = urllib.request.build_opener()
#opener.addheaders = [headers]
#content = opener.open(url).read().decode('utf8')
content = urllib.request.urlopen(url).read().decode('utf-8')
# match regex and drop repetition
raw_hrefs = re.findall(pattern, content, 0)
hset = set(raw_hrefs)

# make directory
if not os.path.exists(proj):
    os.makedirs(proj)
         
         # download links
    for href in hset:
        link = url + href
        print(link)
        try:
            urllib.request.urlretrieve(link, os.path.join('./', proj, href))
        except BaseException as e :
            print(href, e)


from bs4 import BeautifulSoup
import urllib2
import urllib
import pandas as pd
import re
import MySQLdb.cursors
import requests
from requests.exceptions import HTTPError
import sys
import codecs
import json
import time
import re
# get source code of the page
def get_url(url):
  print("HEHEH---------------1")
  user_agent = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7'
  headers = {'User-Agent':user_agent,}
  req = urllib2.Request(url, None,headers)
  try:
    open = urllib2.urlopen(req).read()
    return open
  except urllib2.HTTPError, e:
    print('HTTPError = ' + str(e.code))
  except urllib2.URLError, e:
    print('URLError = ' + str(e.reason))
  except httplib.HTTPException, e:
    print('HTTPException')
  except Exception:
    print("NODONENENNENEE " )
    return "ERROR"
# makes the source tree format like
def beautify(url):
  print(url)
  source = get_url(url)
  time.sleep(4)
  if source == 'ERROR':
    return "ERROR"
  else:

    beauty = BeautifulSoup(source,"html.parser")
    if 'play.google' in url:
      return beauty
    else:
      return beauty

def mysql_connect(db_details):
  host = db_details[0]
  user = db_details[1]
  password = db_details[2]
  dbname = db_details[3]
  try:
    conn = MySQLdb.connect(host,user,password,dbname)
    cur  = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
    return conn,cur
  except:
      return ("Could not connect to mysql")

def fetch_data(query,mysql_cur):
  data = get_data(mysql_cur,query)
  dataArr = data.fetchall()
  print(dataArr)
  return dataArr

def get_data(cursor_obj,query):
  cursor_obj.execute(query)
  return cursor_obj

def load_review(page,url,values):
  values["pageNum"] = str(page)
  data = urllib.urlencode(values)
  req = urllib2.Request(url, data)
  try:
    response = urllib2.urlopen(req)
    time.sleep(5)
    jdata = response.read()
    page = json.loads(jdata[6:])
  except urllib2.HTTPError, e:
    print('HTTPError = ' + str(e.code))
  except urllib2.URLError, e:
    print('URLError = ' + str(e.reason))
  except httplib.HTTPException, e:
    print('HTTPException')
  except Exception:
    print("NODONENENNENEE " )
    return None
  try:
    review = page[0][2]
    return review
  except IndexError:
    return None
  except:
    return None


## _______STTTAAARRRTTTT_______
dbdetails  = ["localhost", "root","root","DWBI"]
mysql_conn,mysql_cur = mysql_connect(dbdetails)

query = "select app from playstore_appdata where  app  not in ( select appname from google_appstore_unstruct )"
dataArr = fetch_data(query,mysql_cur)
for data in dataArr:
  app_search = '%s google APP store app' %(data['app'])
  print(app_search)
  url = urllib.quote(app_search)
  print(url)
  jobs = beautify('http://www.google.com/search?q='+url)
  results_context = jobs.find('div', {'class' : 'g'})
  print("-----------HERE INSIDE ------")
  word = '/play.google.com/store/apps/details'
  Rate_word = 'March'
  links = results_context.findAll('a')
  for a in links:
    if word in a['href']:
      print("   HERE : WORK")
      playstore_url = a['href']
      print(playstore_url)
      play_id = playstore_url.split('play.google.com/store/apps/details')[1]
      print("=============================================================")
      print(play_id)
      try:
        pl_id = play_id.split('%3Fid%3D')[1].split('%26')[0]
      except IndexError:
        print("inside exception IndexError:"  )
        pl_id = "none"
      url_part2 = "https://play.google.com/store/getreviews"
      values = {
          "reviewType": "0", "pageNum": "2",
          "id": pl_id,
          "reviewSortOrder": "2", "xhr": "1"
      }
      page = 0
      sysenc = sys.stdout.encoding
      review = load_review(page,url_part2,values)
      if review is None:
        print("review NONW ")
        continue
      if sysenc == 'cp949':
        review = codecs.encode(review, sysenc, 'ignore')
      beauty_uni = BeautifulSoup(review)
      single_review = beauty_uni.findAll('div', {'class' : 'single-review'})

      for div_review in  single_review:
        div_rate = div_review.find('div', {'class' : 'tiny-star star-rating-non-editable-container'})
        print('---------==========================================-----------------------------')
        str_rate = str(div_rate)
        split_div = str_rate.split('"')[1]
        if not split_div:
          print("NONEEEE")
          continue

        div_name = div_review.find('span', {'class' : 'author-name'})
        str_name = str(div_name)
        split_name = str_name.split('>')[1].split('<')[0]
        div_date = div_review.find('span', {'class' : 'review-date'})
        str_date = str(div_date)
        split_date = str_date.split('>')[1].split('<')[0]
        div_review_2 = div_review.find('div', {'class' : 'review-body with-review-wrapper'})
        with open('testget_data.log', 'w') as f:
          f.write(str(div_review_2))
        str_review = str(div_review_2.encode('utf-8'))
        split_review = str_review.split('>')[3].split('<')[0]
        insert_query = "insert ignore into google_appstore_unstruct(appname ,rating , username , date, review) value ('%s','%s','%s','%s','%s')"%(re.escape(data['app']),split_div,re.escape(split_name),split_date,re.escape(split_review))
        print(insert_query)
        mysql_cur.execute(insert_query)
        mysql_conn.commit()
mysql_cur.close()
mysql_conn.close()

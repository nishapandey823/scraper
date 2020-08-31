

#from twitter_scraper import get_tweets
import pandas as pd
#import mysql.connector
import sqlalchemy
import urllib.request
import pymysql
import time
import numpy as np
from twitter_scraper import get_tweets
import sys
import argparse
import sys
sys.path.append("/instagram/igramscraper/")

import json


import twitterscraper
import random
HEADERS_LIST = [
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; x64; fr; rv:1.9.2.13) Gecko/20101203 Firebird/3.6.13',
    'Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201',
    'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16',
    'Mozilla/5.0 (Windows NT 5.2; RW; rv:7.0a1) Gecko/20091211 SeaMonkey/9.23a1pre'
]
twitterscraper.query.HEADER = {'User-Agent': random.choice(HEADERS_LIST), 'X-Requested-With': 'XMLHttpRequest'}




database_username='root'

database_password='root'
database_ip='127.0.0.1'
database_name= 'scraped_data'
insta_username=''
insta_pass=''

def insertIntoDatabase(df,tableName):

    database_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()
   
   
    a = database_connection.execute("DROP TABLE IF EXISTS "+tableName)


    df.to_sql(con=database_connection, name=tableName, if_exists='append',chunksize=100)

    
    
    query="CREATE TABLE IF NOT EXISTS "+tableName+"_clean   AS (SELECT * FROM "+tableName+" WHERE 1=2)"
    
    
        
    
    a = database_connection.execute(query)
   

    
    query="ALTER TABLE "+tableName+"_clean ADD UNIQUE (id(20));"
    a = database_connection.execute(query)

    database_connection.close()

    
    database_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                                   format(database_username, database_password, 
                                                          database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()
     
    result = database_connection.execute("INSERT IGNORE INTO "+tableName+"_clean SELECT * FROM "+tableName)
    
    
    database_connection.close()


# In[ ]:


#StopDemolitionofPuriMutts
#EmarMutt
#मन्दिर_बचाओ_आन्दोलनम्
#काशी_को_काशी_रहने_दो
#SaveIndicCulture
#KnowYourDeity
#IWantMyMandirBack
#Ab_काशी_मथुरा_की_बारी
#IWantMy40000ThousandMadirBack
#SaveHeritage,#templeheritage,#restoreheritage
#ReclaimKashmirTemples
#reclaimcivilization
#SaveKonark

#AbolishHRCE
#SaveSabarimala
#KL002
#HampiPetition
#WalktoTemple
#RevivalExistence
#HRCE
#SaveOurTemples
#MustVisitTemples
#templelooted
#restoreheritage
#Bringbacktempleglory


# In[8]:



def scrapeTwitter(hashtags,pageCount):
    tweets=[]
    tweet_df = pd.DataFrame()
    print("Scraping ---> "+hashtags)
    twe=get_tweets(hashtags, pages=pageCount)
    for tweet in twe:
        tweet["hashtags"]=tweet['entries']['hashtags']
        tweet["urls"]=tweet['entries']['urls']
        tweet["photos"]=tweet['entries']['photos']
        tweet["videos"]=tweet['entries']['videos']
        tweet.pop('entries')
        tweets.append(tweet)
    
    
    print("For-->"+hashtags+"-->"+ str(len(tweets)))

    df = pd.DataFrame(tweets)  

    #convert array to strings
    if len(tweets)>0:
        df['hashtags'] = [','.join(map(str, l)) for l in df['hashtags']]
        df['photos'] = df.photos.apply(lambda x : None if len(x) ==0 else x[0])
        df['urls'] = [','.join(map(str, l)) for l in df['urls']]
        df['videos'] = [','.join(map(str, l)) for l in df['videos']]
        df['id'] = df['tweetId'].astype(str)
        df.drop_duplicates(subset='tweetId',keep='first', inplace=True)
        
        print("unique records in  "+hashtags+'---> scraped -->'+str(df.shape[0]))
        #tweet_df=tweet_df.append(df, ignore_index = True)
        df['isImageDownloaded']=np.nan
        df['imageName']='NA'
        df['imagePath']=np.nan
        df['keyword']=hashtags
        insertIntoDatabase(df,"twitter")


# In[9]:



# In[49]:


def dowloadImagesTwitter():
   
    database_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                                       format(database_username, database_password, 
                                                              database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()
    a=database_connection.execute("Select photos,id from twitter_clean where photos is not null and isImageDownloaded is null")
    print("Downloading images")
    for row in a:
        
        filename = row[0].split('/')[-1]
        urllib.request.urlretrieve(row[0], "./twitter_images/"+filename)
        database_connection.execute("update twitter_clean set isImageDownloaded=1,imageName='"+filename+ "' where id="+row[1])
        #database_connection.execute("Select photos,twee from twitter_clean where photos is not null limit 100")


# In[65]:


def dowloadImagesInsta():
     
 
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                       format(database_username, database_password, 
                                                              database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()
    a=database_connection.execute("Select image_high_resolution_url,id from instagram_clean where image_high_resolution_url is not null and  isImageDownloaded is null")
    print("Downloading images")

    for row in a:
        filename = row[0].split('/')[-1]
        urllib.request.urlretrieve(row[0], "./instagram_images/"+filename)
        database_connection.execute("update instagram_clean set isImageDownloaded=1,imageName='"+filename+ "' where id="+row[1])
        #database_connection.execute("Select photos,twee from twitter_clean where photos is not null limit 100")


# In[51]:


def dowloadImagesFb():
     

    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                       format(database_username, database_password, 
                                                              database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()
    a=database_connection.execute("Select image,id from fb_clean where image is not null and isImageDownloaded is null")
    print("Downloading images")
    for row in a:
        #filename = row[0].split('/')[-1]
        filename = row[0].split('=')[-1]+".jpg"
        urllib.request.urlretrieve(row[0], "./fb_images/"+filename)
        database_connection.execute("update fb_clean set isImageDownloaded=1,imageName='"+filename+ "' where id="+row[1])
        #database_connection.execute("Select photos,twee from twitter_clean where photos is not null limit 100")


# In[52]:


#facebook
from facebook_scraper import get_posts
def fbData(text,pageCount):
    posts=[]
    for post in get_posts(text, pages=pageCount):
        posts.append(post)

    print("For-->"+text+"-->"+ str(len(posts)))

    df_fb = pd.DataFrame(posts)
    df_fb['id'] = df_fb['post_id'].astype(str)
    df_fb['isImageDownloaded']=np.nan
    df_fb['imageName']='NA'
    df_fb['imagePath']=np.nan
    df_fb.drop_duplicates(subset='post_id',keep='first', inplace=True)
    insertIntoDatabase(df_fb,"fb")


# In[53]:


# In[63]:



from instagram.igramscraper.instagram import Instagram


def intagramScraper(tag,pageCount):

    instagram = Instagram()
    print(insta_username)
    print(insta_pass)
    instagram.with_credentials(insta_username, insta_pass, './')
    instagram.login()
    arr=[]
    medias = instagram.get_medias_by_tag(tag, count=pageCount)
    for media in medias:
        dict={}
        dict['id']=media.identifier
        dict['created_time']=media.created_time
        dict['type']=media.type
        dict['link']=media.link
        dict['caption']=media.caption
        dict['likes_count']=media.likes_count
        dict['comments_count']=media.comments_count
        dict['image_high_resolution_url'] =media.image_high_resolution_url
        account = media.owner
        dict['account'] =account.identifier
        dict['isImageDownloaded']=np.nan
        dict['imageName']='NA'
        dict['imagePath']=np.nan
        # print('Username', account.username)
        # print('Full Name', account.full_name)
        # print('Profile Pic Url', account.get_profile_picture_url_hd())
        arr.append(dict)
    print("for ->>"+ tag +"count->>"+str(len(arr)) )

    df_insta = pd.DataFrame(arr)


    insertIntoDatabase(df_insta,"instagram")

   



# In[22]:


def main():
    n = len(sys.argv) 
    print(n)
    parser = argparse.ArgumentParser()
    parser.add_argument('--site', action="store");
    parser.add_argument('--text', action="store");
    parser.add_argument('--count', action="store");
    parser.add_argument('--download', action="store");
    parser.add_argument('--downloadOnly', action="store");

    args = parser.parse_args();
    print("site = %s" % args.site);
    print("text = %s" % args.text);
    print("count = %s" % args.count);
    print("download = %s" % args.download);
    site= args.site
    text=args.text
    count=args.count
    download=args.download
    downloadOnly=args.downloadOnly

    with open("config.json") as json_data_file:


        data = json.load(json_data_file)
        global database_username 
        database_username=data['mysql']['user']
        global database_password 
        database_password = data['mysql']['password']
        global database_ip 
        database_ip = data['mysql']['host']
        global database_name 
        database_name = data['mysql']['db']
        global insta_username
        insta_username = data['instagram']['username']
        global insta_pass 
        insta_pass = data['instagram']['password']
       
    if downloadOnly==None:
        downloadOnly='false'
    if downloadOnly=='false':
        if args.site and args.text and args.count:

            
            count=int(count)
            if site=='twitter':
                scrapeTwitter(text,count)
                if download=='true':

                    dowloadImagesTwitter();

            elif site=='facebook':
            
                fbData(text,count)
                if download=='true':
                    dowloadImagesFb()

            elif site=='instagram' :
                intagramScraper(text,count)
                if download=='true':
                    dowloadImagesInsta()
            else:
                print("Incorrect input")
            
        else:
            print('Please provide both site , text and count ')
    else:
        if args.site:
             if site=='twitter':

                dowloadImagesTwitter();

             elif site=='facebook':
            
                dowloadImagesFb()

             elif site=='instagram' :
                dowloadImagesInsta()






main()





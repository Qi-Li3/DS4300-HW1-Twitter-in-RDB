import pymysql
from datetime import datetime
import pandas as pd
from mysql.connector import cursor
import timeit
import random as rd

try:
    cnx = pymysql.connect(host='localhost', user='root',
                          password='Dce7A219b',
                          db='tweets', charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)
    cursor = cnx.cursor()

    def postTweets():
        tweetsData = pd.read_csv('//Users/liqi/Desktop/DS4300/hw1_data/tweet.csv', index_col=False, delimiter=',')
        tweetsData.insert(0, 'tweet_id', tweetsData.index+1)
        sql2 = "DELETE FROM tweets.TWEET"
        cursor.execute(sql2)
        for i, row in tweetsData.iterrows():
            sql = "INSERT INTO tweets.TWEET VALUES (%s, %s, %s, now(6))"
            #timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(sql, tuple(row))
            cnx.commit()
            print("Inserted ",i)
        tweetNum = len(tweetsData)
        return tweetNum


    followsData = pd.read_csv('//Users/liqi/Desktop/DS4300/hw1_data/follows.csv', index_col=False, delimiter=',')
    usersList = followsData['USER_ID'].drop_duplicates().to_list()
    del2 = "DELETE FROM tweets.FOLLOWS_"
    cursor.execute(del2)
    for i, row in followsData.iterrows():
        sql = "INSERT INTO tweets.FOLLOWS_ VALUES (%s, %s)"
        cursor.execute(sql, tuple(row))
        cnx.commit()
        print("Inserted ", i)
            # sql3 = "SELECT * FROM tweets.FOLLOWS_"
            # cursor.execute(sql3)
            # # Fetch all the records
            # result = cursor.fetchall()
            # for i in result:
            #     print(i)


    #Execute query
    # sql3 = "SELECT * FROM tweets.FOLLOWS_"
    # cursor.execute(sql3)
    # # Fetch all the records
    # result = cursor.fetchall()
    # for i in result:
    #     print(i)


except pymysql.Error as e:
    print('Error: %d: %s' % (e.args[0], e.args[1]))



try:
    def getHomeTimeline(n):
        for j in range(n):
            user = rd.choice(usersList)
            cursor = cnx.cursor()
            query = "Select tweet_text from TWEET where user_id in (select follows_id from FOLLOWS_ where user_id=%s) order by tweet_ts limit 10"
            cursor.execute(query, user)
            result = cursor.fetchall()
            cursor.close()
            for i in result:
                print(i)

    getHomeTimeline(1)

    # who is following user_id
    def getFollowers(user_id):
        user = [user_id]
        cursor = cnx.cursor()
        query = "Select user_id from FOLLOWS_ where follows_id =%s"
        cursor.execute(query, user)
        result = cursor.fetchall()
        cursor.close()
        for i in result:
            print(i)

    # who is following user_id
    def getFollowees(user_id):
        user = [user_id]
        cursor = cnx.cursor()
        query = "Select follows_id from FOLLOWS_ where user_id =%s"
        cursor.execute(query, user)
        result = cursor.fetchall()
        cursor.close()
        for i in result:
            print(i)

    # tweets posted by user_id
    def getTweets(user_id):
        user = [user_id]
        cursor = cnx.cursor()
        query = "Select tweet_text from TWEET where user_id =%s"
        cursor.execute(query, user)
        result = cursor.fetchall()
        cursor.close()
        for i in result:
            print(i)

except pymysql.Error as e:
    print('Error: %d: %s' % (e.args[0], e.args[1]))


def main():
    start = timeit.default_timer()
    numOfTweets = postTweets()
    stop = timeit.default_timer()
    runtime1 = stop-start
    APIcall1 = numOfTweets/ runtime1

    start = timeit.default_timer()
    n=15
    getHomeTimeline(n)
    stop = timeit.default_timer()
    runtime2 = stop - start
    APIcall2 = n / runtime2

    print("API Method: postTweets              API Calls/Sec: ",APIcall1)
    print("API Method: getHomeTimeline         API Calls/Sec: ", APIcall2)

if __name__ == "__main__":
        main()
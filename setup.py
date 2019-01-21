import re
import tweepy
import sqlite3
import smtplib
import unicodedata
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import yagmail
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from tweepy import OAuthHandler
from textblob import TextBlob
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import quandl
import pandas as pd
import seaborn as sns
sns.set()
# This code downloads the latest daily closing price of a stock index from Quandl, checks if its historical mean average short term has crossed above/below the
# mean average long term and if so collects the latest tweets mentioning the stock index categorized by sentiment and sends out an email with a graph showing the long and short term average and the tweets downloaded.
# The Stock prices are stored in a csv file and the tweets are stored in a SQL table.

class sqlconnect(object):

    def __init__(self):
        pass

    def sqlwrite(self,tweets):
        #This functions stores all new tweets in a SQL table

        # connecting to the SQL database or creating it if not available
        connection = sqlite3.connect("tweettable.db")
        crsr = connection.cursor()

        # table created to temporary store new tweets, created to avoid duplicates
        sql_command_duplicates = """CREATE TABLE IF NOT EXISTS tweethistory_duplicates(
            tweettext VARCHAR(280),
            tweetsentiment VARCHAR(10),
            tweetid INTEGER)"""

        # table created to store new tweets
        sql_command = """CREATE TABLE IF NOT EXISTS tweethistory(
            tweettext VARCHAR(280),
            tweetsentiment VARCHAR(10),
            tweetid INTEGER)"""

        crsr.execute(sql_command)
        crsr.execute(sql_command_duplicates)
        connection.commit()

        for tweet in tweets:

            connection = sqlite3.connect("tweettable.db")

            crsr = connection.cursor()

            # Insert new tweet values temporary to SQL table
            sql_command_duplicates = "INSERT INTO tweethistory_duplicates (tweettext, tweetsentiment, tweetid) VALUES (?, ?, ?)"
            values =(tweet['text'],tweet['sentiment'],tweet['id'])

            crsr.execute(sql_command_duplicates,values)
            connection.commit()

            #Insert new tweet values from temporary SQL table, if tweet id is not already present
            sql_command = "INSERT INTO tweethistory (tweettext, tweetsentiment, tweetid) SELECT tweettext, tweetsentiment, tweetid FROM tweethistory_duplicates WHERE NOT EXISTS (SELECT * FROM tweethistory WHERE tweethistory_duplicates.tweetid = tweethistory.tweetid)"

            crsr.execute(sql_command)
            connection.commit()

        # Delete all data from temporary table
        crsr.execute("DELETE FROM tweethistory_duplicates where tweettext <>0")

        connection.commit()
        connection.close()

class TwitterClient(object):

    #Generic Twitter Class for sentiment analysis.

    def __init__(self):


        # keys and tokens from the Twitter Dev Console
        #API key
        consumer_key = 'yourconsumerkey'
        #API secret key
        consumer_secret = 'yourAPIsecretkey'
        #Access token
        access_token = 'youraccesstoken'
        #Access token secret
        access_token_secret = 'youraccesstokensecret'

        # attempt authentication
        try:
            # create OAuthHandler object
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            # set access token and secret
            self.auth.set_access_token(access_token, access_token_secret)
            # create tweepy API object to fetch tweets
            self.api = tweepy.API(self.auth)
        except:
            print("Error: Authentication Failed")

    def clean_tweet(self, tweet):

        #Utility function to clean tweet text by removing links, special characters using simple regex statements.

        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|(\n)"," ",tweet).split())

    def get_tweet_sentiment(self, tweet):

        #Utility function to classify sentiment of passed tweet using textblob's sentiment method

        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(tweet))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'

    def get_tweets(self, query, count = 10):

        # Main function to fetch tweets and parse them.

        # empty list to store parsed tweets
        tweets = []

        try:
            # call twitter api to fetch tweets
            fetched_tweets = self.api.search(q = query, count = count, lang='en', tweet_mode='extended')

            # parsing tweets one by one
            for tweet in fetched_tweets:
                # empty dictionary to store required params of a tweet
                parsed_tweet = {}

                tweet.full_text = unicodedata.normalize('NFKD', tweet.full_text).encode('ascii','ignore').decode('utf8')
                # saving text of tweet
                parsed_tweet['text'] = tweet.full_text
                # saving sentiment of tweet
                parsed_tweet['sentiment'] = self.get_tweet_sentiment(tweet.full_text)
                # saving id of tweet
                parsed_tweet['id'] = tweet.id

                # appending parsed tweet to tweets list
                if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once
                    if parsed_tweet not in tweets:
                        tweets.append(parsed_tweet)
                else:
                    tweets.append(parsed_tweet)

            # return parsed tweets

            return tweets

        except tweepy.TweepError as e:
            # print error (if any)
            print("Error : " + str(e))

class email():
    def __init__(self):
        pass

    def sendemail(self,tweets):

        # Create list of strings of positive tweets
        body='Below positive tweets:'
        for tweet in [tweet for tweet in tweets if tweet['sentiment'] == 'positive']:
            # Convert to string and delete line break for better layout in email
            tweet = str(tweet['text']).replace("\n"," ")
            body += '\n {}' .format(tweet)

        # Create list of strings of negative tweets
        body +='\n \n Below negative tweets:'
        for tweet in [tweet for tweet in tweets if tweet['sentiment'] == 'negative']:
            tweet = str(tweet['text']).replace("\n"," ")
            body += '\n {}' .format(tweet)

        # Login to gmail account
        yag=yagmail.SMTP('yourgmailaddress@gmail.com','yourgmailpassword')
        # Creating boyd of email with text and picture
        contents = [body, yagmail.inline("yourdpyfiledirectory")]
        # Send to email address
        yag.send('yourgmailaddress@gmail.com', '5 sma has crossed 10 sma', contents)

class quandlclient(object):
    def __init__(self):
        pass
    def collectdata(self,ticker):

        # API key from your Quandl account
        quandl.ApiConfig.api_key = "yourquandlapikey"

        # Calculate todays date and date one month ago
        monthago_date = date.today() - relativedelta(months=1)
        yearago_date = date.today() - relativedelta(years=1)
        today_date = date.today() - relativedelta(days=1)

        #Request data from Quandl
        data = quandl.get(ticker, start_date=yearago_date, end_date=("2018-10-24"))
        # Adding two new columns to panda df with rolling average
        data['rollingmean5'] = data['Index Value'].rolling(window=5).mean()
        data['rollingmean10'] = data['Index Value'].rolling(window=10).mean()

        # Print only the last 3 months of data


        datamonth = data.tail(90)
        datamonth = datamonth.loc[:,['Index Value','rollingmean5','rollingmean10']]
        datamonth.index.sort_values()

        ax  = datamonth.plot()
        # set monthly locator
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=1))
        # set formatter
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
        #ax.text(labelsize=8)
        ax.xaxis.set_tick_params(labelsize=7, labelrotation = 45)
        #ax.xaxis.label.set_size(4)
        ax.set_xlim(datamonth.index[0] - relativedelta(days=1), datamonth.index[-1] + relativedelta(days=1))
        #ax.set_ylim(0, 1)
        # set font and rotation for date tick labels
        plt.gcf().autofmt_xdate()

        # save graph to same directory as .py file
        plt.savefig("graph.png")

        # save dataframe to csv
        data.to_csv('timeseries.csv', sep='\t')

        if datamonth.iloc[-1,datamonth.columns.get_loc('rollingmean5')] > datamonth.iloc[-1,datamonth.columns.get_loc('rollingmean10')] and \
                        datamonth.iloc[-2,datamonth.columns.get_loc('rollingmean5')] < datamonth.iloc[-2,datamonth.columns.get_loc('rollingmean10')] or\
                                datamonth.iloc[-1,datamonth.columns.get_loc('rollingmean5')] < datamonth.iloc[-1,datamonth.columns.get_loc('rollingmean10')] and \
                                datamonth.iloc[-2,datamonth.columns.get_loc('rollingmean5')] > datamonth.iloc[-2,datamonth.columns.get_loc('rollingmean10')] :
            return True
        else:
            return False

def main():

    # creating object for connecting with quandl api
    timeseriesobject = quandlclient()
    # calling function to download daily price level data from quandl, defining ticker to download
    sendemail = timeseriesobject.collectdata("NASDAQOMX/NDX")

    if sendemail == True:

        # creating object for connecting with twitter api
        api = TwitterClient()
        # calling function to get tweets, defining search word and number of tweets to scrape
        tweets = api.get_tweets(query = 'Nasdaq', count = 10)

        # creating object for connecting with SQL database
        sqlobject = sqlconnect()
        # calling function to write tweets to SQL database
        sqlobject.sqlwrite(tweets)

        # creating object for setting up email module
        emailobject =email()
        # calling function to send the time series presented in a graph and tweets
        emailobject.sendemail(tweets)



    else:
        pass


if __name__ == "__main__":
     #calling main function
    main()

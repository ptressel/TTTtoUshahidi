# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4 encoding=utf-8

"""
Provide classes for fetching, iterating over, and tokenizing tweets.

Goal is to de-duplicate tweets that may have been retweeted multiple times.
Tweets may be fetched from multiple sources, such as a Tweak the Tweet csv file
or a Twitter feed.  This hides the sources, and provides tweets in a standard
format.

@author: Pat Tressel
"""

__all__ = ["Tweet", "TtTCSVTweets"]

# @ToDo: This returns source-specific data in an "extra" field (expected to be
# a dict). As an alternative, sources could subclass this class.
class TokenizedTweet:
    """
    Represents one tokenized tweet.
    
    Given a raw tweet and associated data, use this class to tokenize it and
    hold the resulting files.
    
    Has fields for the tweet's sender, its original author (if a retweet),
    the retweet type (RT, MT), length apart from the retweet header,
    and a field for extra data specific to a source.
    """
    
    # Characters that separate tokens
    SEPARATOR_CHARS = ""
    
    # Characters to strip from tokens
    STRIP_CHARS = ""
    
    def __init__(self, tweet, sender, extra=None):
        """
        Create one tokenized tweet.
        
        Given the tweet body, sender, and other data that may be obtained via
        the Twitter API or from TtT, etc., split up the tweet into tokens
        """
        self.tweet = tweet
        self.sender = sender
        self.extra = extra
        
        # Fields we'll construct.
        #self.tokens = tokens
        #self.length = length
        #self.author = author if author else sender
        #self.retweet = retweet

class TweetReader(object):
    """
    Parent class for readers from assorted tweet sources.
    """
    
    def __init__(self):
        pass
    
# @ToDo: Are there other TtT CSV formats? Are header names stable, so we could
# handle other formats by finding the headers we need? For now, check the
# actual headers against the expected headers.
class TweakTheTweetCSVReader(TweetReader):
    """
    Read tweets from a Tweak the Tweet CSV file.
        
    Expects column headings in first row.  Expects 17 columns (see TTT_HEADERS).
    """
    
    # The TtT columns are:
    TTT_HEADERS = ["EVENT", "Report Type", "Report" , "Time - EDT", "Location",
                   "Text", "Contact", "Details", "Date_Time", "Source",
                   "COMPLETE", "GPS_Lat", "GPS_Long", "Photo", "Video",
                   "Author", "ID"]
    
    def __init__(self, csv_file_path):
        """
        Prepare to read tweets from the specified Tweak the Tweet csv file.
        """
        
        # Make sure we can read the csv file by reading the headings.
        import csv
        self.csv_in  = open(csv_file_path, 'rb')
        self.csv_reader = csv.reader(self.csv_in)
        headers = self.csv_reader.next()
        # Is this the format we know about?
        if headers != TweakTheTweetCSVReader.TTT_HEADERS:
            raise ValueError("CSV headings do not match supported format.")
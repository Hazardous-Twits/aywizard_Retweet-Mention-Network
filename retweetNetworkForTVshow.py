#!/usr/bin/python3

# Stand-alone program to draw retweet&mention network
import argparse
import datetime
import logging

import networkx as nx
from pymongo import MongoClient

# Input arguments
PROGRAM_DESCRIPTION = "Draw social network based on retweet and mention"
parser = argparse.ArgumentParser(description=PROGRAM_DESCRIPTION)
parser.add_argument('hashtag', type=str, help='Tv show we are looking for')
args = vars(parser.parse_args())

# Global variables
network = nx.DiGraph()
database = MongoClient()['stream_store']

# Constants
QUOTE_FIELD = "quoted_status"
RETWEET_FIELD = "retweeted_status"
REPLY_FIELD = "in_reply_to_user_id"
ADOPTION_TIMESTAMP = "adoption_time"
RELATION_TIMESTAMP = "creation_time"


def add_user(user_id, timestamp=None):
    # Add node to network, only keep the earliest timestamp
    assert isinstance(user_id, int), "User id must be int!"

    if user_id not in network:
        network.add_node(user_id)

    if timestamp is None:
        return

    if (ADOPTION_TIMESTAMP not in network.node[user_id]) or (timestamp < network.node[user_id][ADOPTION_TIMESTAMP]):
        network.node[user_id][ADOPTION_TIMESTAMP] = timestamp


def add_relation(start, end, edge_type, timestamp):
    assert start in network and end in network, "User id does not exist!"
    assert edge_type in ["R", "M", "Q", "C"], "Edge type must be R(etweet) or M(ention) or Q(uote)"

    if network.has_edge(start, end):
        assert "type" in network[start][end], "Edge must have at least one type!"
        # Update timestamp if it is necessary
        if (RELATION_TIMESTAMP not in network[start][end]) or (timestamp < network[start][end][RELATION_TIMESTAMP]):
            network[start][end][RELATION_TIMESTAMP] = timestamp
        
        if edge_type in network[start][end]["type"]:
            return
        network[start][end]["type"] = network[start][end]["type"] + edge_type
    else:
        network.add_edge(start, end, **{'type': edge_type, RELATION_TIMESTAMP: timestamp})


def query_tweets(hashtag=''):
    """
    Edge: A retweeted B(A follows B)
    A -> B

    Edge: A mentioned B
    A -> B

    If A follows B, A is B's follower, B is A's friend.

    Graph format:
    Node ID: user id in long format
    Node Attribute:
        'adoption_time':     The first time user post a tweet related to the tv show
    Edge Attribute "Type":
        'R':                    Retweet edge
        'M':                    Mention edge
        'Q':                    Quote edge
        'C':                    Reply edge
        "RMQ":                  Retweet and Mention and Quote edge

    """
    assert hashtag != '', "Hashtag shouldn't be empty!"

    logging.info('\nQuery Tweets Begin')
    
    query_string = {"entities.hashtags.text": hashtag}
    logging.info('Query string is ' + str(query_string))

    # Get tweets author list  
    # for tweet in database.tweets.find(query_string).limit(100):
    for tweet in database.tweets.find(query_string):
        # Add user
        user_id = int(tweet['user']['id'])
        create_at = int(datetime.datetime.strptime(
                tweet['created_at'], 
                "%a %b %d %H:%M:%S %z %Y") \
                .timestamp())                
        add_user(user_id, timestamp=create_at)

        original_retweet_mentions = []
        original_quote_mentions = []
        # Retweet
        # Retweet do not have any new content, retweet with comment is called quote.
        # Note: It is possible that A retweet a quote, then tweet will contain both retweet_status and quote_status，
        # Vice Versa.
        if RETWEET_FIELD in tweet:
            original_tweet_id = int(tweet[RETWEET_FIELD]["user"]["id"])
            add_user(original_tweet_id)
            add_relation(user_id, original_tweet_id, "R", create_at)
            # debug_string += "--(R)-->{0} ".format(original_tweet_id)
            original_retweet_mentions = tweet[RETWEET_FIELD]["entities"]["user_mentions"]

        # Quote
        # If the mentions in quote doesn't exist in original tweets,
        # We add edge between current user and mentioned user.
        if QUOTE_FIELD in tweet:
            original_tweet_id = int(tweet[QUOTE_FIELD]["user"]["id"])
            add_user(original_tweet_id)
            add_relation(user_id, original_tweet_id, "Q", create_at)

            original_quote_mentions = tweet[QUOTE_FIELD]["entities"]["user_mentions"]

        # Mentions in retweets is in the original tweet, we discard these mentions.
        if "user_mentions" in tweet["entities"]:
            tweet_mentions = tweet["entities"]["user_mentions"]
            for mentioned_user in tweet_mentions:
                try:
                    # user id could be none
                    mentioned_user_id = int(mentioned_user["id"])
                except:
                    logging.error("Abnormal mention id in tweet {0}".format(tweet["id"]))
                    continue

                if mentioned_user in original_retweet_mentions or mentioned_user in original_quote_mentions:
                    continue
                add_user(mentioned_user_id)
                add_relation(user_id, mentioned_user_id, "M", create_at)
                # Count mention
                user_network = database.social_network.find_one({"userid": user_id})
                mention_network = database.social_network.find_one({"userid": mentioned_user_id})

        # Reply
        if REPLY_FIELD in tweet and tweet[REPLY_FIELD] is not None:
            reply_user_id = int(tweet[REPLY_FIELD])
            add_user(reply_user_id)
            add_relation(user_id, int(tweet[REPLY_FIELD]), "C", create_at)

    print("Done")


def draw_and_save(filename='draw.graphml'):
    nx.write_graphml(network, filename)


if __name__ == "__main__":
    logging.basicConfig(filename='drawRetweetMentionNetwork.log', level=logging.INFO, format='%(asctime)s %(message)s')
    tv_show = args['hashtag']
    logging.info('Query begin\n hashtag:{0}'.format(tv_show))

    query_tweets(hashtag=tv_show)
    logging.info("Network has {0} nodes, {1} edges".format(network.number_of_nodes(), network.number_of_edges()))
    
    draw_and_save(filename=tv_show + '.graphml')

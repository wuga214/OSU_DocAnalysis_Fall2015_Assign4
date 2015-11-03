#
# util module
#
# util module provides three methods: parse_tweet parses tweet record and return 
# <user_name, list_of_mentioned_users, list_of_hashtags>; has_hashtag checks whether a tweet contains a hashtag; 
# write_graph writes edges to a .csv file. 
#
# Author: Liping Liu
# 
#





from __future__ import print_function

import string 
import re 
from operator import add

def parse_tweet(tweet):

    # pattern of user name in the second line of the record
    match = re.search('\nU\thttp.*twitter.com/(\\w+)\n', tweet)

    user = "??????" 
    mentioned_users = []
    hashtags = []

    if not (match is None):
        user = match.group(1)

    # pattern of mentioned user
    mentioned_users = re.findall('@([_\w]+)', tweet) 
    # pattern of hashtag 
    hashtags = re.findall('#([_\w]+)', tweet) 

    return (user, mentioned_users, hashtags)

# This function determines whether a tweet contains the hashtag 
def has_hashtag(tweet, tag): 
    match = re.search("#" + tag + "[^\w]", tweet) 
    return (not (match is None)) 

# This function writes edge list to csv file
def write_graph(edge_list, filename): 
    
    graph_file = open(filename, "w")
    for edge in edge_list:
        #In the edge_list, each tuple "edge" is collected from my MapReduce solution. 
        #Each edge is like this: <"user1,user2", 2>. The key value is already a string 
        #with two names concatenated   
        #Think about this question: how to avoid key-value pairs like this: <"user1,user2", 1> <"user2,user1", 1>?
        #If you don't avoid that, the reducer would not treat the two pairs as the same edge. 
        graph_file.write(edge[0] + "," + edge[1])

    graph_file.close()


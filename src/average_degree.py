# example of program that calculates the average degree of hashtags
from pybloomfilter import BloomFilter
import logging
import json
import sys
# from itertools import permutations
from itertools import combinations
from tweets_cleaned import remove_non_ascii
import time
import logging
from logging import getLogger
import enum
from enum import Enum

# can use heapq instead if tweets will be inputted out of order
class Queue:
    def __init__(self):
        self.items = []
    def peek(self):
        if(len(self.items)>0):
            return self.items[len(self.items)-1]
        return None
    def isEmpty(self):
        return self.items == []
    def enqueue(self, item):
        self.items.insert(0,item)
    def dequeue(self):
        return self.items.pop()
    def size(self):
        return len(self.items)
    
class LogLevels(Enum):
    read_input_and_generate_graph = 9
    evict_hashtags = 8
    evict_timestamps =7
    avg_degree_and_prune = 6
    update_degree = 5
    add_edges =4
    add_single_edge = 3
    remove_edges = 2
    remove_single_edge = 1
     

class WindowAvgDegree(object):
    
    # python 2 no enum support
    
    infname = "../tweet_input/tweets.txt"
    outfname = "../tweet_output/ft1.txt"
    NUM_MS_IN_60s = 60000
    logging.addLevelName(1,"process_tw")

    degree=0
    degree_of_current_node =0
#     LogLevels = enum(
#                      remove_single_edge=9
#               ,remove_edges=8
#             ,add_single_edge=7
#             ,add_edges=6
#             ,update_degree=5
#             ,avg_degree_and_prune=4
#             ,evict_timestamps=3
#             ,evict_hashtags=2
#             ,process_tweet=1
#                      )
    logger = logging.getLogger("WindowAvgDegree")
    
    def init_logger(self,logger):
        logger.setLevel(logging.DEBUG)
        
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    
    def __init__(self,infilename="", outfilename=""):
        self.init_logger(self.logger)
        self.infname = infilename
        self.outfname = outfilename
        self.reset_datastructures()
        
    def get_jsonifiable_graph(self,graph):
        return { k : list(v) for k,v in self.graph.items() if v}
    
    def reset_datastructures(self):
        self.degree = 0
        
        # singletons
        self.degree_of_current_node = 0;
        self.number_of_evictions = 0;
        self.evicted_time_stamps = []
        
        # python dictionary are hashtables (no ordering)
        self.time_dict = {}
        
        # therefore, want to use a queue to keep time order
        self.time_queue = Queue()
        
        # the hashtag graph
        self.graph = {}
            
    # deprecated, do not use
#     def extract_hashtags(self,tweet):
#         hashtags= set(pt[1:] for pt in tweet.split() if pt.startswith('#'))
#         return [item.lower() for item in hashtags]
    
    def remove_single_edge(self,graph,hashtags):
        num_edges_removed = 0
        prev_hashtag = None
        for hashtag in hashtags:
            if not prev_hashtag == None:
                if prev_hashtag in graph and hashtag in graph[prev_hashtag] :
                    graph[prev_hashtag].remove(hashtag)
                    num_edges_removed +=1
            prev_hashtag = hashtag
        return num_edges_removed
    
    # given a list of hashtags list, remove their edges from graph
    def remove_edges(self,graph,hashtags_list):
        removed_edges = 0
        for hashtags in hashtags_list:  
            # generate edges
            for c in combinations(hashtags,2):
                combo = list(c)
                removed_edges +=self.remove_single_edge(graph, combo)
                removed_edges +=self.remove_single_edge(graph, reversed(combo))
        has_removed_edges = True if removed_edges>0 else False
        return has_removed_edges
    
    def add_single_edge(self,graph,hashtags):
        num_edges_added = 0
        prev_hashtag = None
        for hashtag in hashtags:
            if not prev_hashtag==None:
                if not prev_hashtag in graph.keys():
                    graph[prev_hashtag] = set()
                if(not hashtag in graph[prev_hashtag]):
                    graph[prev_hashtag].add(hashtag)
                    num_edges_added += 1
            prev_hashtag = hashtag
        return num_edges_added
    
    # using permutations to ensure bidirectional connection
    def add_edges(self,graph,hashtags):
        num_edges_added = 0
        # generate edges
        for c in combinations(hashtags,2):
            combo = list(c)
            num_edges_added +=self.add_single_edge(graph,combo)
            num_edges_added +=self.add_single_edge(graph,reversed(combo))
        has_added_edges =  True if num_edges_added>0 else  False
        return has_added_edges
    
    def update_degree(self,key,value):
        self.degree_of_current_node+= len(value)
#         self.jsonifiable_graph[key] = list(value)
        return value
    
    # because the values in the dict is a set, it's not jsonifiable
    
    # method to calculate graph degree
    # remove nodes that no longer have connections
    def avg_degree_and_prune(self):
        # update singleton variable (count of connected nodes)
        self.degree_of_current_node = 0;
        # update graph
        self.graph = { k : self.update_degree(k,v) for k,v in self.graph.items() if v}
        # return degree
        self.degree =  round(self.degree_of_current_node/len(self.graph.keys()),2)
        self.logger.setLevel(LogLevels.avg_degree_and_prune.value)
        self.logger.log(LogLevels.avg_degree_and_prune.value,self.degree)
        return self.degree
    
    def evict_timestamps(self, curr_timestamp):
        evicted_timestamps = []
        while (not self.time_queue.isEmpty() 
               and (curr_timestamp - self.time_queue.peek() > self.NUM_MS_IN_60s)):
            # eviction with dequeue
            evicted_timestamps.append(self.time_queue.dequeue())
        if evicted_timestamps:
            lvl =int(LogLevels.evict_timestamps.value)
            self.logger.setLevel(lvl)
            self.logger.log(lvl,evicted_timestamps)
        return evicted_timestamps

    def evict_hashtags(self,evicted_timestamps):
        evicted_hashtags = []
        for timestamp in evicted_timestamps:
            # eviction with pop
            evicted_hashtags.append(self.time_dict.pop(timestamp,[]))
        
        if evicted_hashtags:
            lvl =int(LogLevels.evict_hashtags.value)
            self.logger.setLevel(lvl)
            self.logger.log(lvl,evicted_hashtags)
        return evicted_hashtags
        
    def clean_hashtag(self,hashtag):
        (_,newhashtag) = remove_non_ascii(hashtag)
        return newhashtag.lower().strip()
    
    # extract & clean up hashtags from the tweet
    def extract_hash_tags(self,tweet):
        hashtags = []
        for x in tweet['entities']['hashtags']:
            cleanedhashtag = self.clean_hashtag(x['text'])
            if len(cleanedhashtag)>0:
                hashtags.append(cleanedhashtag)
        return hashtags
    
    # process a single tweet
    def process_tweet(self,tweet):
        if not tweet['entities']['hashtags']:
            return self.degree  # becaseu there are no hashtags
        
        hashtags = self.extract_hash_tags(tweet)
        curr_timestamp = int(tweet['timestamp_ms'])
        evicted_timestamps = self.evict_timestamps(curr_timestamp)
        evicted_hashtags = self.evict_hashtags(evicted_timestamps)
        self.evicted_time_stamps = evicted_timestamps
        
        has_removed_edges = False
        if evicted_hashtags and evicted_timestamps:
            self.number_of_evictions+=1
            has_removed_edges = self.remove_edges(self.graph, evicted_hashtags)
#          
        has_added_edges = False
        if len(hashtags)>1:
            # add edges to graph
            has_added_edges  = self.add_edges(self.graph, hashtags)
          
            # enqueue time
            self.time_queue.enqueue(curr_timestamp)
              
            # store hashtags related to timestamp
            self.time_dict[curr_timestamp] = hashtags
          
        # update degree
        if has_removed_edges or has_added_edges: 
            self.degree = self.avg_degree_and_prune()
        return self.degree
        
    def read_input_and_generate_graph(self):
        # reset everything on a new run of this method
        self.reset_datastructures()
        target = open(self.outfname,'w')
        target.truncate()
        with open(self.infname, 'r+') as f:
            for tweet_json in f:
                tweet = json.loads(tweet_json)
                if 'text' in tweet and 'timestamp_ms' in tweet:
                    self.process_tweet(tweet)
                    target.write(str(self.degree)+"\n")
                    
            # log the outcome
            self.logger.setLevel(LogLevels.read_input_and_generate_graph.value)
            self.logger.log(LogLevels.read_input_and_generate_graph.value,json.dumps(self.get_jsonifiable_graph(self.graph)))
        target.close()
        

def main():
    print("processing")
    
    start = time.time()
    
    deg = WindowAvgDegree(sys.argv[1],sys.argv[2])
    deg.read_input_and_generate_graph()
#     deg.nothing()
    
    end = time.time()
    elapsed =  end-start

    print("done in "+str(elapsed)+"s")

if __name__ == "__main__":
    main()

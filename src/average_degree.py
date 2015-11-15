# example of program that calculates the average degree of hashtags
from pybloomfilter import BloomFilter
import logging
import json
import sys
import re
from itertools import permutations
from itertools import combinations
from tweets_cleaned import remove_non_ascii
import datetime
import time

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

class WindowAvgDegree(object):
    infname = "../tweet_input/tweets.txt"
    outfname = "../tweet_output/ft1.txt"
    num_ms_in_60s = 60000
    degree=0
    degree_of_current_node =0
    
    def __init__(self,infilename, outfilename):
        self.infname = infilename
        self.outfname = outfilename
        self.reset_datastructures()
    
    def reset_datastructures(self):
        self.degree = 0
        self.degree_of_current_node = 0;
        self.number_of_evictions = 0;
        
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
        has_removed_edge = False
        prev_hashtag = None
        for hashtag in hashtags:
            if not prev_hashtag == None:
                if prev_hashtag in graph and hashtag in graph[prev_hashtag] :
                    graph[prev_hashtag].remove(hashtag)
                    has_removed_edge = True
            prev_hashtag = hashtag
        return has_removed_edge
    
    # given a list of hashtags list, remove their edges from graph
    def remove_edges(self,graph,hashtags_list):
        has_removed_edges = False
        for hashtags in hashtags_list:
            for p in permutations(hashtags):
                has_removed_edges =self.remove_single_edge(graph, p)
        return has_removed_edges
    
    def add_single_edge(self,graph,hashtags):
        has_added_edges = False
        prev_hashtag = None
        for hashtag in hashtags:
            if not prev_hashtag==None:
                if not prev_hashtag in graph.keys():
#                     graph[prev_hashtag] = BloomFilter(10000,0.01,'filter.bloom')
                    graph[prev_hashtag] = set()
                if(not hashtag in graph[prev_hashtag]):
                    graph[prev_hashtag].add(hashtag)
                    has_added_edges = True
            prev_hashtag = hashtag
        return has_added_edges
    
    
    # using permutations to ensure bidirectional connection
    def add_edges(self,graph,hashtags):
        added_edges = False
        for p in permutations(hashtags):
            added_edges =self.add_single_edge(graph, p)
        return added_edges
    #     hashtags.append(hashtags[len(hashtags)-1])
    #     for i in range(len(hashtags),1,-1):
    #         for c in combinations(hashtags,i):
    #             add_single_edge(tweet,c)
    
    
    def update_degree(self,value):
        self.degree_of_current_node+= len(value)
        return value
    
    # method to calculate graph degree
    # remove nodes that no longer have connections
    def avg_degree_and_prune(self):
        # update singleton variable (count of connected nodes)
        self.degree_of_current_node = 0;
        # update graph
        self.graph = { k : self.update_degree(v) for k,v in self.graph.items() if v}
        # return degree
        return round(self.degree_of_current_node/len(self.graph.keys()),2)
    
    def evict_timestamps(self, curr_timestamp):
        evicted_timestamps = []
        while (not self.time_queue.isEmpty() 
               and (curr_timestamp - self.time_queue.peek() > self.num_ms_in_60s)):
            # eviction with dequeue
            evicted_timestamps.append(self.time_queue.dequeue())
        return evicted_timestamps

    def evict_hashtags(self,evicted_timestamps):
        evicted_hashtags = []
        for timestamp in evicted_timestamps:
            # eviction with pop
            evicted_hashtags.append(self.time_dict.pop(timestamp,[]))
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
        
        has_removed_edges = False
        if evicted_hashtags and evicted_timestamps:
            self.number_of_evictions+=1
            print(str(self.number_of_evictions)+" "+evicted_hashtags.__str__())
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
        self.reset_datastructures()
        target = open(self.outfname,'w')
        target.truncate()
        with open(self.infname, 'r+') as f:
            for tweet_json in f:
                tweet = json.loads(tweet_json)
                if 'text' in tweet and 'timestamp_ms' in tweet:
                    self.process_tweet(tweet)
                    target.write(str(self.degree)+"\n")
        target.close()
    
def main():
    print("processing")
    
    start = time.time()
    
    deg = WindowAvgDegree(sys.argv[1],sys.argv[2])
    deg.read_input_and_generate_graph()
    
    end = time.time()
    elapsed =  end-start

    print("done in "+str(elapsed)+"s")

if __name__ == "__main__":
    main()

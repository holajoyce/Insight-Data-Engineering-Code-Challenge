# example of program that calculates the average degree of hashtags

import logging
import json
import sys
import re
from itertools import permutations
from itertools import combinations


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
    
    def __init__(self):
        self.reset_datastructures()
    
    def reset_datastructures(self):
        # python dictionary are hashtables (no ordering)
        self.time_dict = {}
        
        # use a queue to keep time order
        self.time_queue = Queue()
        
        # the hashtag graph
        self.graph = {}
            
    # clean a single tweet if needed
    def extract_hashtags(self,tweet):
        hashtags= set(pt[1:] for pt in tweet.split() if pt.startswith('#'))
        return [item.lower() for item in hashtags]
    
    def remove_single_edge(self,graph,hashtags):
        has_removed_edge = False
        prev_hashtag = None
        for hashtag in hashtags:
            if not prev_hashtag == None:
                if hashtag in graph[prev_hashtag] :
                    graph[prev_hashtag].remove(hashtag)
                    has_removed_edge = True
            prev_hashtag = hashtag
        return has_removed_edge
         
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
                    graph[prev_hashtag] = set()
                if(not hashtag in graph[prev_hashtag]):
                    graph[prev_hashtag].add(hashtag)
                    has_added_edges = True
            prev_hashtag = hashtag
        return has_added_edges
        
    def add_edges(self,graph,hashtags):
        added_edges = False
        for p in permutations(hashtags):
            added_edges =self.add_single_edge(graph, p)
        return added_edges
    #     hashtags.append(hashtags[len(hashtags)-1])
    #     for i in range(len(hashtags),1,-1):
    #         for c in combinations(hashtags,i):
    #             add_single_edge(tweet,c)
    
    def avg_degree(self,graph):
        num_nodes = len(graph.keys())
        degrees = 0
        for related_nodes in graph.values():
            degrees +=  len(related_nodes)
        return round(degrees/num_nodes,2)
    
    def find_evicted_timestamps(self, curr_timestamp):
        evicted_timestamps = []
        while (not self.time_queue.isEmpty() 
               and (curr_timestamp - self.time_queue.peek() > self.num_ms_in_60s)):
            evicted_timestamps.append(self.time_queue.dequeue())
        return evicted_timestamps

    def find_evicted_hashtags(self,evicted_timestamps):
        evicted_hashtags = []
        for timestamp in evicted_timestamps:
            hashtags = self.time_dict.pop(timestamp,[])
            evicted_hashtags.append(hashtags)
        return evicted_hashtags
        
    def process_tweet(self,tweet):
        if not tweet['entities']['hashtags']:
            return
        hashtags = [x['text'] for x in tweet['entities']['hashtags']]
        curr_timestamp = int(tweet['timestamp_ms'])
        evicted_timestamps = self.find_evicted_timestamps(curr_timestamp)
        evicted_hashtags = self.find_evicted_hashtags(evicted_timestamps)
        
        # TODO
        has_removed_edges = False
        if evicted_hashtags:
            has_removed_edges = self.remove_edges(self.graph, evicted_hashtags)
        
        has_added_edges = False
        if len(hashtags)>1:
            # add edges to graph
            has_added_edges  = self.add_edges(self.graph, hashtags)
        
            # enqueue time
            self.time_queue.enqueue(curr_timestamp)
            
            # store hashtags related to timestamp
            self.time_dict[curr_timestamp] = hashtags
            
        if has_removed_edges or has_added_edges: 
            self.degree = self.avg_degree(self.graph)
            
        return self.degree
        
    def read_and_graph(self,infname,outfname):
        self.reset_datastructures()
        target = open(outfname,'w')
        target.truncate()
        with open(self.infname, 'r+') as f:
            for tweet_json in f:
                tweet = json.loads(tweet_json)
                if 'text' in tweet and 'timestamp_ms' in tweet:
                    degree = self.process_tweet(tweet)
                    target.write(str(degree)+"\n")
        target.close()
    
    
def main():
    print("processing")
    deg = WindowAvgDegree()
    deg.read_and_graph(sys.argv[1],sys.argv[2])
    print("done")

if __name__ == "__main__":
    main()

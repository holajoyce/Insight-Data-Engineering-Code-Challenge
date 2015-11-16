'''
Created on Nov 15, 2015

@author: private
'''
# from average_degree import WindowAvgDegree
from testfixtures import log_capture
import unittest
import json
import sys
import logging
from logging import getLogger
from average_degree import WindowAvgDegree 
from average_degree import LogLevels
import collections

# class Hi(object):
#     
#     logger = getLogger("Hi")
#     logger.addHandler(logging.StreamHandler())
#     logger.setLevel(logging.DEBUG)
#     
#     def hi_method(self):
#         print("hey")
#         self.logger.info("hihi")
#         self.logger.warning("heyhey")
#         

compare = lambda x, y: collections.Counter(x) == collections.Counter(y)

class WindowAvgDegreeTest(unittest.TestCase):
    
    infname = "../tweet_input/tweets.txt"
    outfname = "../tweet_output/ft1.txt"
    deg = WindowAvgDegree(infname,outfname)
    
    @log_capture(level=LogLevels.evict_timestamps.value)
    def test_evict_timestamps(self,l):
        self.deg.read_input_and_generate_graph()
        evicted_timestamp  = l.records[4].msg[0]
        print("evicted timestamp: "+str(evicted_timestamp))
        self.assertEqual(evicted_timestamp,1446141061000)

    @log_capture(level=LogLevels.evict_hashtags.value)
    def test_evict_hashtags(self,l):
        self.deg.read_input_and_generate_graph()
        evicted_hashtag = l.records[5].msg
        print("evicted_hashtags: "+evicted_hashtag)
        self.assertEqual(evicted_hashtag,"1 [['spark', 'apache']]")

    @log_capture(level=LogLevels.process_tweet.value)
    def test_get_avg_degree(self,l):
        self.deg.read_input_and_generate_graph()
        ending_avg_degree = l.records[5].msg
        desired = float(1.67)
        print("ending avg degree: "+str(ending_avg_degree))
        self.assertAlmostEqual(ending_avg_degree,desired,2)



if __name__ == '__main__':
    unittest.main()
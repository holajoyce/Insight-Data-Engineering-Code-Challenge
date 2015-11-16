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
    
    def find_log_lines(self,l,level_num):
        log_lines = []
        for x in l.records:
            if x.levelno == level_num:
                log_lines.append(x.msg)
        return log_lines
     
#     def setUp(self):
#         self.deg = WindowAvgDegree(self.infname,self.outfname)
#         self.deg.read_input_and_generate_graph()
    
    @log_capture(level=LogLevels.evict_timestamps.value)
    def test_evict_timestamps(self,l):
        self.deg.read_input_and_generate_graph()
        log_lines = self.find_log_lines(l,l.level)
        self.assertEqual(log_lines[0][0],1446141061000)
  
    @log_capture(level=LogLevels.evict_hashtags.value)
    def test_evict_hashtags(self,l):
        self.deg.read_input_and_generate_graph()
        log_lines = self.find_log_lines(l,l.level)
        self.assertEqual(log_lines[0][0][0],"spark")
        self.assertEqual(log_lines[0][0][1],"apache")
  
    @log_capture(level=LogLevels.avg_degree_and_prune.value)
    def test_get_avg_degree(self,l):
        self.deg.read_input_and_generate_graph()
        log_lines = self.find_log_lines(l, l.level)
        self.assertAlmostEqual(log_lines[-1],float(1.67),2)

    @log_capture(level=LogLevels.read_input_and_generate_graph.value)
    def test_graph(self,l):
        self.deg.read_input_and_generate_graph()
        log_lines = self.find_log_lines(l, l.level)
        compare(json.loads(log_lines[len(log_lines)-1]), json.loads('{"spark": ["flink", "hbase"], "storm": ["apache", "hadoop"], "apache": ["storm", "hadoop"], "flink": ["spark"], "hbase": ["spark"], "hadoop": ["storm", "apache"]}'))

if __name__ == '__main__':
    unittest.main()
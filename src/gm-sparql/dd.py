'''
Created on 2014. 11. 5.

@author: Seokyong Hong
'''
from graph import DegreeDistribution as DegreeDistributionModule
from graph import Eccentricity as EccentricityModule
from graph import TriangleCountDirected as TriangleCountDirectedModule
from graph import TriangleCountUndirected as TriangleUndirectedModule
from graph import TriangleCountUndirectedPerNode as TriangleUndirectedPerNodeModule
from graph import TriangleCountMultithread as TriangleCountMultithreadModule
from graph import ConnectedComponent as ConnectedComponentModule
from graph import PageRank as PageRankModule
from graph import TestAlgorithm as TestAlgorithmModule
from graph import PageRankNoUpdateMax as PageRankNoUpdateMaxModule
from PLUS import FusekiLogin
import logging
import time
import random

logging.basicConfig(level = logging.CRITICAL)

def testDegreeDistribution():
    algorithm = DegreeDistributionModule.DegreeDistribution()
    algorithm.initialize()
    algorithm.process()        
    algorithm.finalize()

def testEccentricity(init_node):
    algorithm = EccentricityModule.Eccentricity()
    algorithm.initialize(init_node)
    algorithm.process()
    algorithm.finalize()

def testTriangleCountDirected():
    algorithm = TriangleCountDirectedModule.TriangleCountDirected()
    algorithm.initialize()
    algorithm.process()  
    algorithm.finalize() 

def testTriangleCountUndirectedPerNode():
    algorithm = TriangleUndirectedPerNodeModule.TriangleCountUndirectedPerNode()
    algorithm.initialize()
    algorithm.process()
    algorithm.finalize() 

def testTriangleCountUndirected():
    algorithm = TriangleUndirectedModule.TriangleCountUndirected()
    algorithm.initialize()
    algorithm.process()
    algorithm.finalize() 

def testTriangleCountMultithread():
    algorithm = TriangleCountMultithreadModule.TriangleCountMultithread()
    algorithm.initialize()
    algorithm.process()
    algorithm.finalize()

def testConnectedComponent():
    algorithm = ConnectedComponentModule.ConnectedComponent()
    algorithm.initialize()
    algorithm.process()
    algorithm.finalize()
    
def testPageRank(directed):
    algorithm = PageRankModule.PageRank()
    algorithm.initialize(0.85, 50, directed)
    algorithm.process(directed)
    algorithm.finalize()

def testTestAlgorithm():
    algorithm = TestAlgorithmModule.TestAlgorithm()
    algorithm.initialize(0.85, 50)
    results = algorithm.process()
    if results is not None:
        for result in results['results']['bindings']:
            print 'Vertex: ' + result['vertex']['value'] + ', PageRank: ' + result['pagerank']['value']
    algorithm.finalize()

def testPageRankNoUpdateMax(directed):
    algorithm = PageRankNoUpdateMaxModule.PageRankNoUpdateMax()
    algorithm.initialize(0.85, 50, directed)
    algorithm.process(directed)
    algorithm.finalize()

def getRandomNode():
    connection = FusekiLogin('ds', 'solarpy')
    query_to_get_random_nodes = "select distinct ?s {?s ?p ?o} offset "+str(random.randint(0,18772))+"limit 1"
    results = connection.urika.query("get_rand_node", query_to_get_random_nodes, None, None, 'json', True)
    rand_node = ""
    for result in results['results']['bindings']:
        rand_node = "<"+str(result['s']['value'])+">"
    print rand_node
    return rand_node

if __name__ == '__main__':
    

    print "* Computing Degree Distribution ... "
    startTime = time.time()
    testDegreeDistribution()
    endTime = time.time()
    print 'Elapsed Time: ' + str(endTime - startTime) + ' seconds.'
    print ""





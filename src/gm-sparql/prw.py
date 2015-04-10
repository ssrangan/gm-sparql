'''
List of ORNL team members (in alphabetical order)
* Sangkeun Lee (lees4@ornl.gov, leesangkeun@gmail.com)
* Seung-Hwan. Lim (lims1@ornl.gov)
* Seokyong Hong (shong3@ncsu.edu)
* Sreenivas R. Sukumar (sukumarsr@ornl.gov)
* Tyler C. Brown (browntc@ornl.gov)

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
from graph import RandomWalkParallel as RandomWalkModule
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


def testPageRankNoUpdateMax(directed):
    algorithm = PageRankNoUpdateMaxModule.PageRankNoUpdateMax()
    algorithm.initialize(0.85, 50, directed)
    algorithm.process(directed)
    algorithm.finalize()

def testRandomWalk():
    algorithm = RandomWalkModule.RandomWalk()
    algorithm.initialize(LENGTH_OF_PATH,NO_OF_THREADS, MAX_ITERATION)
    algorithm.process()        
    algorithm.finalize()


if __name__ == '__main__':
    
    # this is a beta version k-parallel random walk

    LENGTH_OF_PATH = 10;
    NO_OF_THREADS = 1000; 
    MAX_ITERATION = 30;
    print "* Running Random Walk ... "
    startTime = time.time()
    testRandomWalk()
    endTime = time.time()
    print 'Elapsed Time: ' + str(endTime - startTime) + ' seconds.'
    print ""





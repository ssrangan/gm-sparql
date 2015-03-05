'''
List of ORNL team members (in alphabetical order)
* Sangkeun Lee (lees4@ornl.gov, leesangkeun@gmail.com)
* Seung-Hwan. Lim (lims1@ornl.gov)
* Seokyong Hong (shong3@ncsu.edu)
* Sreenivas R. Sukumar (sukumarsr@ornl.gov)
* Tyler C. Brown (browntc@ornl.gov)

'''
from graph import GraphDiffusion as GraphDiffusionModule

from PLUS import FusekiLogin
import logging
import time
import random

logging.basicConfig(level = logging.CRITICAL)

def testGraphDiffusion(startNode, initialScore, edgeWeight, maxIteration):
    algorithm = GraphDiffusionModule.GraphDiffusion()
    algorithm.initialize(startNode, initialScore)
    algorithm.process(edgeWeight, maxIteration)
    algorithm.finalize()

def getRandomNode():
    
    # this function is written for simple testing and it is dependent on data sets
    # please use carefully

    connection = FusekiLogin('ds', 'solarpy')
    query_to_get_random_nodes = "select distinct ?s {?s ?p ?o} offset "+str(random.randint(0,6))+"limit 1"
    results = connection.urika.query("get_rand_node", query_to_get_random_nodes, None, None, 'json', True)
    rand_node = ""
    for result in results['results']['bindings']:
        rand_node = "<"+str(result['s']['value'])+">"
    print rand_node
    return rand_node

if __name__ == '__main__':
    
    print "* Computing Graph Diffusion (Beta) ... "
    startTime = time.time()

    # current graph diffusion algorithm is implemented assuming a homogeneous graph 
    # where nodes are connected by a single edge type <urn:connectedTo>

    testGraphDiffusion(startNode = getRandomNode(), initialScore = 100.0, edgeWeight = 0.95, maxIteration = 5)

    endTime = time.time()
    print 'Elapsed Time: ' + str(endTime - startTime) + ' seconds.'
    print ""




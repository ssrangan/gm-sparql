'''
List of ORNL team members (in alphabetical order)
* Sangkeun Lee (lees4@ornl.gov, leesangkeun@gmail.com)
* Seung-Hwan. Lim (lims1@ornl.gov)
* Seokyong Hong (shong3@ncsu.edu)
* Sreenivas R. Sukumar (sukumarsr@ornl.gov)
* Tyler C. Brown (browntc@ornl.gov)

'''
from graph import PeerPressureClustering as PeerPressureClusteringModule

from PLUS import FusekiLogin
import logging
import time
import random

logging.basicConfig(level = logging.CRITICAL)

def testPeerPressureClustering(maxIteration):
    algorithm = PeerPressureClusteringModule.PeerPressureClustering()
    algorithm.initialize()
    algorithm.process(maxIteration)
    algorithm.finalize()

if __name__ == '__main__':
    

    print "* Computing Peer Pressure Clustering ... "
    startTime = time.time()
    testPeerPressureClustering(maxIteration = 10)
    endTime = time.time()
    print 'Elapsed Time: ' + str(endTime - startTime) + ' seconds.'
    print ""




'''
Created on 2014. 11. 24.

@author: Seokyong Hong
'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from threading import Thread
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule

counter = []

class Worker(Thread):
    def __init__(self, name, query, connection):
        Thread.__init__(self)
        self.name = name
        self.query = query
        self.connection = connection
    
    def run(self):
        global counter
        
        try :
            results = self.connection.urika.query(self.name, self.query, None, None, 'json', True)
        except ConnectionError:
            print 'No connection could be made.'
        else:
            counter.append(int(results['results']['bindings'][0]['count']['value']))
    
class TriangleCountMultithread(GraphAlgorithmModule.GraphAlgorithm):
    SUBQUERIES = [
                  '''
                      SELECT (COUNT(*) AS ?count)
                          WHERE {
                            ?x ?a ?y .
                            ?y ?b ?z .
                            ?z ?c ?x
                            FILTER (STR(?x) < STR(?y))
                            FILTER (STR(?y) < STR(?z))
                          }
                  '''
                  ,
                  '''
                      SELECT (COUNT(*) AS ?count)
                          WHERE {
                            ?x ?a ?y .
                            ?y ?b ?z .
                            ?z ?c ?x
                            FILTER (STR(?y) > STR(?z))
                            FILTER (STR(?z) > STR(?x))
                          }
                  '''
                  ,
                  '''
                      SELECT (COUNT(*) AS ?count)
                          WHERE {
                            ?x ?a ?y .
                            ?y ?b ?z .
                            ?x ?c ?z
                          }
                  '''
    ] 
    
    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Multithread Triangle Count Directed')
    
    def initialize(self):
        self.connection = FusekiLogin('ds', 'solarpy')
        self.workers = []
        for worker in range(0, 3):
            self.workers.append(Worker('Worker-' + str(worker + 1), TriangleCountMultithread.SUBQUERIES[worker], self.connection))
    
    def process(self):
        for worker in self.workers:
            worker.start();
            
        for worker in self.workers:
            worker.join()
    
    def finalize(self):
        global counter
        summation = 0
        for count in counter:
            summation = summation + count
        
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()
        writer.writeLine('Number of Triangles: ' + str(summation) + '.')
        writer.close()
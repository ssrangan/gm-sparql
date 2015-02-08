'''
Created on 2014. 11. 6.

@author: Seokyong Hong
'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule

class TriangleCountUndirectedPerNode(GraphAlgorithmModule.GraphAlgorithm):
    QUERY = '''
                SELECT ?x (COUNT(DISTINCT *) AS ?count)
                    WHERE
                    {
                        { ?x ?p ?y . } UNION { ?y ?p ?x . }
                        { ?y ?p ?z . } UNION { ?z ?p ?y . }
                        { ?z ?p ?x . } UNION { ?x ?p ?z . }
                        FILTER(STR(?x) < STR(?y))
                        FILTER(STR(?y) < STR(?z))
                    }
                GROUP BY ?x
            '''
    
    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Triangle Count Undirected Per Node')
    
    def initialize(self):
        self.connection = FusekiLogin('ds', 'solarpy')
    
    def process(self):
        try :
            self.results = self.connection.urika.query(self.name, TriangleCountUndirectedPerNode.QUERY, None, None, 'json', True)
        except ConnectionError:
            print 'No connection could be made.'
            self.results = None
    
    def finalize(self):
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()
        for result in self.results['results']['bindings']:
            writer.writeLine('Vertex: ' + result['x']['value'] + ', Triagle Count: ' + result['count']['value'])
        writer.close()
        
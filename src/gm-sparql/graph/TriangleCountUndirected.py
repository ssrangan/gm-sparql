'''
Created on 2014. 11. 6.

@author: Seokyong Hong
'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule

class TriangleCountUndirected(GraphAlgorithmModule.GraphAlgorithm):
    QUERY = '''
                SELECT (COUNT(DISTINCT *) AS ?count)
                    WHERE
                    {
                        { ?x ?p ?y . } UNION { ?y ?p ?x . }
                        { ?y ?p ?z . } UNION { ?z ?p ?y . }
                        { ?z ?p ?x . } UNION { ?x ?p ?z . }
                        FILTER(STR(?x) < STR(?y))
                        FILTER(STR(?y) < STR(?z))
                    }
            '''
    
    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Triangle Count Undirected')
    
    def initialize(self):
        self.connection = FusekiLogin('ds', 'solarpy')
    
    def process(self):
        try :
            results = self.connection.urika.query(self.name, TriangleCountUndirected.QUERY, None, None, 'json', True)
            self.count = results['results']['bindings'][0]['count']['value']
        except ConnectionError:
            print 'No connection could be made.'
            self.count = -1
    
    def finalize(self):
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()
        writer.writeLine('Number of Triangles: ' + self.count + '.')
        writer.close()
        
'''
Created on 2014. 11. 14.

@author: Seokyong Hong
'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule

class TriangleCountDirected(GraphAlgorithmModule.GraphAlgorithm):
    QUERY = '''
                SELECT (COUNT(*) AS ?count)
                    WHERE {
                        {
                            ?x ?a ?y .
                            ?y ?b ?z .
                            ?z ?c ?x
                            FILTER (STR(?x) < STR(?y))
                            FILTER (STR(?y) < STR(?z))
                        }
                        UNION 
                        {
                            ?x ?a ?y .
                            ?y ?b ?z .
                            ?z ?c ?x
                            FILTER (STR(?y) > STR(?z))
                            FILTER (STR(?z) > STR(?x))
                        }  
                        UNION 
                        {
                            ?x ?a ?y .
                            ?y ?b ?z .
                            ?x ?c ?z
                        }
                    }
            '''
    
    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Triangle Count Directed')
    
    def initialize(self):
        self.connection = FusekiLogin('ds', 'solarpy')
    
    def process(self):
        try :
            results = self.connection.urika.query(self.name, TriangleCountDirected.QUERY, None, None, 'json', True)
            self.count = results['results']['bindings'][0]['count']['value']
        except ConnectionError:
            print 'No connection could be made.'
            self.count = -1
    
    def finalize(self):
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()
        writer.writeLine('Number of Triangles: ' + self.count + '.')
        writer.close()
        
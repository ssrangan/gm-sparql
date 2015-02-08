'''
Created on 2014. 11. 5.

@author: Seokyong Hong
'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule

class DegreeDistribution(GraphAlgorithmModule.GraphAlgorithm):
    QUERY = '''
                SELECT ?degree (COUNT (?degree) AS ?frequency)
                    WHERE {
                        SELECT (COUNT (?target) AS ?degree) 
                            WHERE {
                                { ?target ?out_edge ?out_node }
                                UNION
                                { ?in_node ?in_edge ?target }
                            }
                            GROUP BY ?target
                    }
                    GROUP BY ?degree
            '''
    
    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Degree Distribution')
        
    def initialize(self):
        self.connection = FusekiLogin('ds', 'solarpy')
    
    def process(self):
        try :
            self.results = self.connection.urika.query(self.name, DegreeDistribution.QUERY, None, None, 'json', True)
        except ConnectionError:
            print 'No connection could be made.'
            self.results = None

    def finalize(self):
        if self.results is not None:
            writer = FileWriterModule.FileWriter(self.name)
            writer.open()
            
            for result in self.results['results']['bindings']:
                writer.writeLine('Degree: ' + result['degree']['value'] + ', Frequency: ' + result['frequency']['value'])
            
            writer.close()
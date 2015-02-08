'''
Created on 2014. 11. 8.

@author: Seokyong Hong
'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule
import time

class ConnectedComponent(GraphAlgorithmModule.GraphAlgorithm):
    COMMAND_TO_INITIALIZE_WORKING_GRAPH = 'CREATE GRAPH <http://workingGraph>'
    COMMAND_TO_FINALIZE_WORKING_GRAPH = 'DROP GRAPH <http://workingGraph>'
    COMMAND_TO_INSERT_NODES_TO_WORKING_GRAPH = '''
                                                INSERT { GRAPH <http://workingGraph> { ?vertex <temp:labels> ?vertex . }}
                                                    WHERE
                                                    {
                                                        { ?vertex <urn:connectedTo> ?o . }
                                                        UNION
                                                        { ?s <urn:connectedTo> ?vertex . }
                                                    }
                                               '''
    COMMAND_TO_UPDATE_NODE_TO_MINIMAL_NODE = '''
                                                DELETE { GRAPH <http://workingGraph> { ?s <temp:counts> ?o . }}
                                                    WHERE { GRAPH <http://workingGraph> { ?s <temp:counts> ?o . }};

                                                DELETE { GRAPH <http://workingGraph> { ?node <temp:labels> ?original . }}
                                                INSERT { GRAPH <http://workingGraph> { ?node <temp:labels> ?update ; <temp:counts> 1 . }}
                                                    WHERE
                                                    {    
                                                        {
                                                            SELECT ?node (MIN(?label) AS ?update)
                                                            WHERE
                                                            {
                                                                {
                                                                    { GRAPH <http://workingGraph>
                                                                        { ?node <temp:labels> ?minimum . }
                                                                    }
                                                                    { ?node <urn:connectedTo> ?neighbor . }
                                                                    UNION
                                                                    { ?neighbor <urn:connectedTo> ?node . }
                                                                    { GRAPH <http://workingGraph>
                                                                        { ?neighbor <temp:labels> ?label. }
                                                                    }
                                                                    FILTER (STR(?minimum) > STR(?label))
                                                                }
                                                                UNION
                                                                { GRAPH <http://workingGraph>
                                                                    { ?node <temp:labels> ?label . }
                                                                }
                                                            }
                                                            GROUP BY ?node
                                                        }
                                                        { GRAPH <http://workingGraph>
                                                            { ?node <temp:labels> ?original . }
                                                        }
                                                        FILTER (?original != ?update)
                                                    }
                                             '''
    COMMAND_TO_CHECK_CONVERGENCE = '''
                                    SELECT (COUNT(*) as ?changed)
                                        WHERE
                                        {
                                            { GRAPH <http://workingGraph>
                                                { ?vertex <temp:counts> ?count}
                                            }
                                        }
                                   '''
    COMMAND_TO_RETRIEVE_RESULT = 'SELECT * WHERE { GRAPH <http://workingGraph> { ?node <temp:labels> ?label . }}'
    
    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Connected Component')
        
    def initialize(self):
        self.connection = FusekiLogin('ds', 'solarpy')
        try:
            self.connection.urika.update(self.name, ConnectedComponent.COMMAND_TO_INITIALIZE_WORKING_GRAPH)
            self.connection.urika.update(self.name, ConnectedComponent.COMMAND_TO_INSERT_NODES_TO_WORKING_GRAPH)
        except ConnectionError:
            print 'No connection could be made.'
    
    def process(self):
        iteration = 0
        while True:
            start = time.time()
            self.connection.urika.update(self.name, ConnectedComponent.COMMAND_TO_UPDATE_NODE_TO_MINIMAL_NODE)
            end = time.time()
            print 'Iteration ' + str(iteration) + ': ' + str(end - start) + ' seconds'
            iteration+=1
            if self.isConverged():
                break
    
    def finalize(self):
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()
        
        try:
            results = self.connection.urika.query(self.name, ConnectedComponent.COMMAND_TO_RETRIEVE_RESULT)
            for result in results['results']['bindings']:
                writer.writeLine('Node: ' + result['node']['value'] + " [Label: " + result['label']['value'] + "]")
    
            self.connection.urika.update(self.name, ConnectedComponent.COMMAND_TO_FINALIZE_WORKING_GRAPH)
        except ConnectionError:
            print 'No connection could be made.'
        finally:
            writer.close()

    def isConverged(self):
        count = 0
        converged = False
        results = self.connection.urika.query(self.name, ConnectedComponent.COMMAND_TO_CHECK_CONVERGENCE)
        for result in results['results']['bindings']:
            count = int(result['changed']['value'])

        if count == 0:
            converged = True
       
        return converged 
'''
Created on 2014. 3. 5.

@ author: Sangkeun Lee
@ peer-pressure clustering

'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule
import time

class PeerPressureClustering(GraphAlgorithmModule.GraphAlgorithm):
    COMMAND_TO_INITIALIZE_WORKING_GRAPH = 'CREATE GRAPH <http://workingGraph>; CREATE GRAPH <http://labelCnt>'
    COMMAND_TO_FINALIZE_WORKING_GRAPH = 'DROP GRAPH <http://workingGraph>; DROP GRAPH <http://labelCnt>'
    COMMAND_TO_INSERT_NODES_TO_WORKING_GRAPH = '''
                                                INSERT { GRAPH <http://workingGraph> { ?vertex <temp:labels> ?vertex . }}
                                                    WHERE
                                                    {
                                                        { ?vertex <urn:connectedTo> ?o . }
                                                        UNION
                                                        { ?s <urn:connectedTo> ?vertex . }
                                                    }
                                               '''
    COMMAND_TO_COUNT_ADJ_LABELS = '''
                                            DELETE { GRAPH <http://labelCnt> { ?s ?p ?o . }}
                                                    WHERE { GRAPH <http://labelCnt> { ?s ?p ?o . }};

                                            insert {graph <http://labelCnt> {?node ?adj_label ?cnt}} where {
                                            select ?node ?adj_label (count(*) as ?cnt) {
                                            select (?s as ?node) ?p (?o as ?adj) ?adj_label
                                            {
                                                  {{?s ?p ?o.} union {?o ?p ?s}}
                                                  {graph <http://workingGraph> {?o <temp:labels>?adj_label}}
                                            } } group by ?node ?adj_label
                                            }
                                '''       

    COMMAND_TO_UPDATE_NODE_TO_PEER_MAJORITY_NODE = '''
                                                DELETE { GRAPH <http://workingGraph> { ?s <temp:counts> ?o . }}
                                                    WHERE { GRAPH <http://workingGraph> { ?s <temp:counts> ?o . }};

                                                DELETE { GRAPH <http://workingGraph> { ?node <temp:labels> ?original . }}
                                                INSERT { GRAPH <http://workingGraph> { ?node <temp:labels> ?update ; <temp:counts> 1 . }}
                                                    WHERE
                                                    {    
                                                        {
                                                            select ?node (min(?adj_label) as ?update) {{
                                                            select ?node (max(?cnt) as ?maxClusCnt) {                
                                                            select ?node ?adj_label ?cnt {graph <http://labelCnt> {?node ?adj_label ?cnt}}
                                                            } group by ?node
                                                            }
                                                            {
                                                            select ?node ?adj_label ?cnt {graph <http://labelCnt> {?node ?adj_label ?cnt}}
                                                            }
                                                            filter(?cnt=?maxClusCnt)
                                                            } group by ?node
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
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Peer Pressure Clustering')
        
    def initialize(self):
        self.connection = FusekiLogin('ds', 'solarpy')
        try:
            self.connection.urika.update(self.name, PeerPressureClustering.COMMAND_TO_INITIALIZE_WORKING_GRAPH)
            self.connection.urika.update(self.name, PeerPressureClustering.COMMAND_TO_INSERT_NODES_TO_WORKING_GRAPH)
        except ConnectionError:
            print 'No connection could be made.'
    
    def process(self, maxIteration):
        iteration = 0
        while True:
            start = time.time()
            self.connection.urika.update(self.name, PeerPressureClustering.COMMAND_TO_COUNT_ADJ_LABELS)
            self.connection.urika.update(self.name, PeerPressureClustering.COMMAND_TO_UPDATE_NODE_TO_PEER_MAJORITY_NODE)
            end = time.time()
            print 'Iteration ' + str(iteration) + ': ' + str(end - start) + ' seconds'
            iteration+=1
            if self.isConverged() or iteration==maxIteration:
                break
    
    def finalize(self):
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()
        
        try:
            results = self.connection.urika.query(self.name, PeerPressureClustering.COMMAND_TO_RETRIEVE_RESULT)
            for result in results['results']['bindings']:
                writer.writeLine('Node: ' + result['node']['value'] + " [Label: " + result['label']['value'] + "]")
    
            self.connection.urika.update(self.name, PeerPressureClustering.COMMAND_TO_FINALIZE_WORKING_GRAPH)
        except ConnectionError:
            print 'No connection could be made.'
        finally:
            writer.close()

    def isConverged(self):
        count = 0
        converged = False
        results = self.connection.urika.query(self.name, PeerPressureClustering.COMMAND_TO_CHECK_CONVERGENCE)
        for result in results['results']['bindings']:
            count = int(result['changed']['value'])

        if count == 0:
            converged = True
       
        return converged 
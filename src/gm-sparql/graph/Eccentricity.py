'''
Created on 2014. 11. 23.

@author: Seokyong Hong
'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule
import time

class Eccentricity(GraphAlgorithmModule.GraphAlgorithm):
    COMMAND_TO_INITIALIZE_WORKING_GRAPH = 'CREATE GRAPH <http://workingGraph>'
    COMMAND_TO_FINALIZE_WORKING_GRAPH = 'DROP GRAPH <http://workingGraph>'
    COMMAND_TO_INSERT_INITIAL_NODE = 'INSERT {{ GRAPH <http://workingGraph> {{ {0} <temp:labels> {1} . }}}} WHERE {{}}'
    COMMAND_TO_INSERT_NEIGHBOR_NODES = '''
                                        INSERT {{ GRAPH <http://workingGraph> {{ ?neighbor <temp:labels> {0} . }} }}
                                            WHERE 
                                            {{
                                                SELECT ?neighbor ?label
                                                WHERE 
                                                {{
                                                    {{ GRAPH <http://workingGraph> {{ ?node <temp:labels> {1} . }} }}
                                                    {{
                                                        {{ ?node <urn:connectedTo> ?neighbor . }}
                                                        UNION
                                                        {{ ?neighbor <urn:connectedTo> ?node . }}
                                                    }}
                                                    NOT EXISTS {{ GRAPH <http://workingGraph> {{ ?neighbor<temp:labels> ?any . }} }}
                                                }}
                                            }}           
                                        '''
    COMMAND_TO_COUNT_NODES_IN_WORKING_GRAPH = 'SELECT (COUNT(*) AS ?count) WHERE { GRAPH <http://workingGraph> { ?s ?p ?o }}'
    
    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Eccentricity')
    
    def initialize(self, targetNode):
        self.connection = FusekiLogin('ds', 'solarpy')
        self.targetNode = targetNode
        
        try:
            self.connection.urika.update(self.name, Eccentricity.COMMAND_TO_INITIALIZE_WORKING_GRAPH)
            self.connection.urika.update(self.name, Eccentricity.COMMAND_TO_INSERT_INITIAL_NODE.format(targetNode, 0))
        except ConnectionError:
            print 'No connection could be made.'
    
    def process(self):
        iteration = 0
        self.count = 0
        shouldStop = False
        previousCount = self.counts()
        
        while not shouldStop:
            startTime = time.time()
            self.connection.urika.update(self.name, Eccentricity.COMMAND_TO_INSERT_NEIGHBOR_NODES.format(iteration + 1, iteration))
            currentCount = self.counts()
            endTime = time.time()
            print 'Elapsed Time Iteration ' + str(iteration + 1) + ': ' + str(endTime - startTime) + ' seconds.'
            if previousCount == currentCount:
                shouldStop = True
                break
            
            previousCount = currentCount
            iteration = iteration + 1
            
        self.iteration = iteration
    
    def finalize(self):
        try:
            self.connection.urika.update(self.name, Eccentricity.COMMAND_TO_FINALIZE_WORKING_GRAPH)
        except ConnectionError:
            print 'No connection could be made.'
        
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()
        writer.writeLine('The eccentricity of ' + self.targetNode + ' is ' + str(self.iteration) + '.')
        writer.close()
    
    def counts(self):
        results = self.connection.urika.query(self.name, Eccentricity.COMMAND_TO_COUNT_NODES_IN_WORKING_GRAPH, None, None, 'json', True)
        
        for result in results['results']['bindings']:
            retrievedCount = int(result['count']['value'])
            
        return retrievedCount
'''
Created on 2014. 11. 24.

@author: Seokyong Hong
'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
import time

class TestAlgorithm(GraphAlgorithmModule.GraphAlgorithm):
    COMMAND_TO_RETRIEVE_NUMBER_OF_VERTICES = '''
                                                SELECT (COUNT(DISTINCT ?vertex) AS ?count) 
                                                    WHERE {
                                                        { ?vertex <urn:connectedTo> [] . }
                                                        UNION
                                                        { [] <urn:connectedTo> ?vertex . } 
                                                    }
                                             '''
    
    COMMAND_TO_INITIALIZE_WORKING_GRAPH = 'CREATE GRAPH <http://workingGraph>'
    COMMAND_TO_FINALIZE_WORKING_GRAPH = 'DROP GRAPH <http://workingGraph>'
    COMMAND_TO_STORE_OUT_DEGREES = '''
                                        INSERT { GRAPH <http://workingGraph> { ?vertex <temp:outDegree> ?outDegree . }}
                                            WHERE {
                                                SELECT ?vertex (COUNT(*) AS ?outDegree)
                                                    WHERE {
                                                        { ?vertex <urn:connectedTo> ?object . }
                                                    }
                                                    GROUP BY ?vertex
                                            }
                                   '''
    COMMAND_TO_STORE_INITIAL_PAGERANKS = '''
                                            INSERT {{ GRAPH <http://workingGraph> {{ ?vertex <temp:pagerank> {0} . }}}}
                                                WHERE {{
                                                    SELECT DISTINCT ?vertex 
                                                        WHERE {{
                                                            {{ ?vertex <urn:connectedTo> [] . }}
                                                            UNION
                                                            {{ [] <urn:connectedTo> ?vertex . }}
                                                        }}
                                                }}
                                         '''
    COMMAND_TO_UPDATE_PAGERANKS = '''
                                    DELETE {{ GRAPH <http://workingGraph> {{ ?vertex <temp:difference> ?previousDifference . }}}}
                                        WHERE 
                                        {{
                                            GRAPH <http://workingGraph> {{ ?vertex <temp:difference> ?previousDifference . }}
                                        }};
    
                                    DELETE {{ GRAPH <http://workingGraph> {{ ?vertex <temp:pagerank> ?previousRank . }}}}
                                    INSERT {{ GRAPH <http://workingGraph> {{ ?vertex <temp:pagerank> ?newRank ; <temp:difference> ?difference . }}}}
                                        WHERE    
                                        {{
                                            {{
                                                SELECT ?vertex ?previousRank (SUM(?neighborRank / ?degree) * {DAMPING_FACTOR} + {STAYING_FACTOR} AS ?newRank) (ABS(?previousRank - ?newRank) AS ?difference)
                                                    WHERE
                                                    {{
                                                        {{
                                                            {{ GRAPH <http://workingGraph>
                                                                {{ ?vertex <temp:pagerank> ?previousRank . }}
                                                            }}
                                                            {{ ?neighbor <urn:connectedTo> ?vertex . }}
                                                        }}
                                                        {{ GRAPH <http://workingGraph> 
                                                            {{ ?neighbor <temp:outDegree> ?degree ; <temp:pagerank> ?neighborRank . }}
                                                        }}
                                                    }}
                                                    GROUP BY ?vertex ?previousRank 
                                                    HAVING (?newRank != ?previousRank)
                                            }}
                                        }};
                                  '''

    COMMAND_TO_CALCULATE_MAX_DIFFERENCE = '''
                                                SELECT (MAX(?difference) AS ?maxDifference)
                                                    WHERE 
                                                    { GRAPH <http://workingGraph>
                                                        { ?vertex <temp:difference> ?difference . }
                                                    }
                                          '''
    COMMAND_TO_GATHER_RESULT = '''
                                    SELECT ?vertex ?pagerank
                                        WHERE { GRAPH <http://workingGraph>
                                            {
                                                ?vertex <temp:pagerank> ?pagerank . 
                                            }
                                        }
                                        ORDER BY DESC(?pagerank)
                               '''
    
    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'PageRank')
        
    def initialize(self, dampingFactor, limitIteration):
        self.connection = FusekiLogin('ds', 'solarpy')
        self.dampingFactor = dampingFactor
        self.limitIteration = limitIteration
        results = self.connection.urika.query(self.name, TestAlgorithm.COMMAND_TO_RETRIEVE_NUMBER_OF_VERTICES, None, None, 'json', True) 
        
        self.numberOfVertices = int(results['results']['bindings'][0]['count']['value'])
        self.convergenceThreshold = 1.0 / float(self.numberOfVertices) / 10.0 
        self.connection.urika.update(self.name, TestAlgorithm.COMMAND_TO_INITIALIZE_WORKING_GRAPH)
        self.connection.urika.update(self.name, TestAlgorithm.COMMAND_TO_STORE_OUT_DEGREES)
        self.connection.urika.update(self.name, TestAlgorithm.COMMAND_TO_STORE_INITIAL_PAGERANKS.format(1.0 / float(self.numberOfVertices)))
    
    def process(self):
        for iteration in range(0, self.limitIteration):
            start = time.time()
            self.connection.urika.update(self.name, TestAlgorithm.COMMAND_TO_UPDATE_PAGERANKS.format(DAMPING_FACTOR = self.dampingFactor, STAYING_FACTOR = ((1.0 - self.dampingFactor) / float(self.numberOfVertices))))
            end = time.time()
            print "Iteration Time: " + str(end - start)
            if self.isConverged():
                break
            
        return self.connection.urika.query(self.name, TestAlgorithm.COMMAND_TO_GATHER_RESULT, None, None, 'json', True)
    
    def finalize(self):
        self.connection.urika.update(self.name, TestAlgorithm.COMMAND_TO_FINALIZE_WORKING_GRAPH)
    
    def isConverged(self):
        start = time.time()
        results = self.connection.urika.query(self.name, TestAlgorithm.COMMAND_TO_CALCULATE_MAX_DIFFERENCE, None, None, 'json', True)
        end = time.time()
        print "Convergence Chcking Time: " + str(end - start)
        difference = float(results['results']['bindings'][0]['maxDifference']['value'])
        return difference < self.convergenceThreshold
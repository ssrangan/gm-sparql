'''
Created on 2015. 3. 5.

@ author: Sangkeun Lee
@ Graph Diffusion
'''
from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule
import time

class GraphDiffusion(GraphAlgorithmModule.GraphAlgorithm):
    COMMAND_TO_INITIALIZE_WORKING_GRAPH = 'DROP GRAPH <http://workingGraph>; CREATE GRAPH <http://workingGraph>'
    COMMAND_TO_FINALIZE_WORKING_GRAPH = 'DROP GRAPH <http://workingGraph>'
    COMMAND_TO_INSERT_INITIAL_NODE = 'INSERT {{ GRAPH <http://workingGraph> {{ {0} <temp:score0> {1} . }}}} WHERE {{}}'
    COMMAND_TO_DIFFUSE_SCORES_PAPER = '''
                                        insert {{graph <http://workingGraph> {{?o <temp:score{0}> ?new_score}}}} where {{
                                        select ?o (sum(?new_score_elem) as ?new_score){{
                                        select ?o ((?curr_score_s * {2}) as ?new_score_elem) {{
                                            {{?s ?p ?o}} union {{?o ?p ?s}}
                                            {{graph <http://workingGraph>{{?s <temp:score{1}> ?curr_score_s}}}}}}
                                        }} group by ?o
                                        }};

                                        DELETE {{ GRAPH <http://workingGraph> {{ ?s <temp:score{1}> ?o . }}}}
                                                    WHERE {{ GRAPH <http://workingGraph> {{ ?s <temp:score{1}> ?o . }}}};
                                        '''
    COMMAND_TO_CREATE_SCORE_ELEM = '''
                                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> 
                                    drop graph <http://tempScoreElem>;
                                    create graph <http://tempScoreElem>;
                                    insert {{ GRAPH <http://tempScoreElem> {{?o <temp:ScoreElem> ?new_score_elem}} }} where
                                    {{
                                    select ?o ((xsd:float(?curr_score_s) * {1}) as ?new_score_elem) 
                                                    {{
                                                        {{?s ?p ?o}} union {{?o ?p ?s}}
                                                        {{graph <http://workingGraph>{{?s <temp:score{0}> ?curr_score_s}}}}
                                                    }}
                                    }}
                                    '''

    COMMAND_TO_DIFFUSE_SCORES = '''
                                        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> 
                                        insert {{graph <http://workingGraph> {{?o <temp:score{0}> ?new_score}}}} where {{
                                        
                                        select ?o ((?new_score_sum/?cnt) as ?new_score){{
                                            {{
                                                select ?o (sum(?new_score_elem) as ?new_score_sum) 
                                                {{
                                                    select ?o ?new_score_elem 
                                                    {{
                                                        {{graph <http://tempScoreElem>{{?o ?has ?new_score_elem}}}}
                                                    }}
                                                }} group by ?o
                                            }}
                                            {{
                                                select (count(*) as ?cnt) {{
                                                    {{graph <http://tempScoreElem>{{?o ?has ?new_score_elem}}}}
                                                }}
                                            }}
                                        }}

                                        }};

                                        DELETE WHERE {{ GRAPH <http://workingGraph> {{ ?s <temp:score{1}> ?o . }}}};

                                        '''
    COMMAND_TO_DIFFUSE_SCORES_WORKING = '''
                                        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> 
                                        insert {{graph <http://workingGraph> {{?o <temp:score{0}> ?new_score}}}} where {{
                                        
                                        select ?o ((?new_score_sum/?cnt) as ?new_score){{
                                            {{
                                                select ?o (sum(?new_score_elem) as ?new_score_sum) 
                                                {{
                                                    select ?o ((xsd:float(?curr_score_s) * {2}) as ?new_score_elem) 
                                                    {{
                                                        {{?s ?p ?o}} union {{?o ?p ?s}}
                                                        {{graph <http://workingGraph>{{?s <temp:score{1}> ?curr_score_s}}}}
                                                    }}
                                                }} group by ?o
                                            }}
                                            {{
                                                select (count(*) as ?cnt) {{
                                                    {{?s ?p ?o}} union {{?o ?p ?s}}
                                                    {{graph <http://workingGraph>{{?s <temp:score{1}> ?curr_score_s}}}}}}
                                            }}
                                        }}

                                        }};

                                        DELETE WHERE {{ GRAPH <http://workingGraph> {{ ?s <temp:score{1}> ?o . }}}};

                                        '''                                                                         
    COMMAND_TO_GATHER_RESULT = '''
                                    SELECT ?vertex ?score
                                        WHERE { GRAPH <http://workingGraph>
                                            {
                                                ?vertex ?has ?score . 
                                            }
                                        }
                                        ORDER BY DESC(?score)
                               '''
    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Graph Diffusion')
    
    def initialize(self, startNode, initialScore):
        self.connection = FusekiLogin('ds', 'solarpy')
        self.targetNode = startNode
        
        try:
            self.connection.urika.update(self.name, GraphDiffusion.COMMAND_TO_INITIALIZE_WORKING_GRAPH)
            self.connection.urika.update(self.name, GraphDiffusion.COMMAND_TO_INSERT_INITIAL_NODE.format(startNode, initialScore))
        except ConnectionError:
            print 'No connection could be made.'
    
    def process(self, edgeWeight, maxIteration):
        iteration = 0
        self.count = 0
        
        while True:
            startTime = time.time()
            self.connection.urika.update(self.name, GraphDiffusion.COMMAND_TO_CREATE_SCORE_ELEM.format(iteration, edgeWeight))
            self.connection.urika.update(self.name, GraphDiffusion.COMMAND_TO_DIFFUSE_SCORES.format(iteration + 1, iteration, edgeWeight))
            endTime = time.time()
            print 'Elapsed Time Iteration ' + str(iteration + 1) + ': ' + str(endTime - startTime) + ' seconds.'
            if iteration + 1== maxIteration:
                break

            iteration = iteration + 1
            
        self.iteration = iteration
    
    def finalize(self):
        
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()
        
        results = self.connection.urika.query(self.name, GraphDiffusion.COMMAND_TO_GATHER_RESULT, None, None, 'json', True)
        for result in results['results']['bindings']:
            writer.writeLine('Vertex: ' + result['vertex']['value'] + ', Score: ' + result['score']['value'])

        writer.close()
        self.connection.urika.update(self.name, GraphDiffusion.COMMAND_TO_FINALIZE_WORKING_GRAPH)
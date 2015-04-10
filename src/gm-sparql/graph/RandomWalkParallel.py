from graph import GraphAlgorithm as GraphAlgorithmModule
from PLUS import FusekiLogin
from requests.exceptions import ConnectionError
from utility import FileWriter as FileWriterModule
import time
import random

class RandomWalk(GraphAlgorithmModule.GraphAlgorithm):
    COMMAND_TO_INITIALIZE_WORKING_GRAPH = '''
    CREATE GRAPH <http://workingGraph>; 
    CREATE GRAPH <http://workingGraph1>; 
    CREATE GRAPH <http://workingGraph2>
                      '''
    COMMAND_TO_FINALIZE_WORKING_GRAPH = '''
    DROP SILENT GRAPH <http://workingGraph>;
    DROP SILENT GRAPH <http://workingGraph1>;                        
    DROP SILENT GRAPH <http://workingGraph2>
                    '''
    COMMAND_TO_RETRIEVE_NUMBER_OF_SOURCES = '''
    SELECT (COUNT(DISTINCT ?vertex) AS ?count) 
    WHERE {
              { ?vertex ?p ?o  }
          }
                                            '''
    COMMAND_TO_INITIALIZE_RESULTS = '''
    INSERT {GRAPH <http://workingGraph> 
               {?vertex <temp:counts> ?count;
                        <temp:percentage> ?percentage; 
                        <temp:difference> ?difference.}
           }
    WHERE{
         SELECT DISTINCT ?vertex (0 AS ?count) (0 AS ?percentage) (10 AS ?difference)
         WHERE {
                   {?vertex ?connect ?object.}
                   UNION{?o ?p ?vertex}
               }
         }
                                     '''
    COMMAND_TO_GET_START_NODES = '''
    INSERT {{GRAPH<http://workingGraph2> {{ ?pathId <step:0> ?startNode . }} }}
    WHERE{{

             {{       
             SELECT (UUID() AS ?pathId) ?startNode
                 WHERE{{
                          {{

                              SELECT DISTINCT ?startNode 
                              WHERE{{
                                       {{?startNode ?p ?o}}

                                       UNION{{?o ?p ?startNode}} 
                   }}
                          }}

                BIND(RAND() AS ?sortKey)

                }} 
                ORDER BY ?sortKey
                LIMIT {0}
             }}

    }}
                    '''
    COMMAND_TO_RANDOM_WALK = '''
    INSERT {{GRAPH<http://workingGraph2> {{?pathId <step:{0}> ?nextNode. }} }}
    WHERE{{

              SELECT ?pathId (SAMPLE(?_nextNode) AS ?nextNode)
          {{
              SELECT ?pathId ?currentNode ?_nextNode
          {{

              GRAPH<http://workingGraph2>{{?pathId <step:{1}> ?currentNode}}
                      ?currentNode ?p ?_nextNode

              BIND(RAND() AS ?orderKey)
          }}
          ORDER BY ?orderKey
          }}

          GROUP BY ?pathId
      }}
                 '''
    COMMAND_TO_UPDATE_PATH_RANK = '''
    DELETE {GRAPH<http://workingGraph2>
               {?pathId ?p ?o}
       }
    INSERT {GRAPH<http://workingGraph1>

           {?pathId ?p ?o}

       }
    WHERE{
         SELECT ?pathId ?p ?o
         WHERE{GRAPH<http://workingGraph2>

                      {?pathId ?p ?o}
          }
     };


    DELETE {GRAPH<http://workingGraph>
           {?vertex <temp:counts> ?previousCount; 

            <temp:percentage> ?previousPercentage; 
            <temp:difference> ?previousDiff 
           }
       }
    INSERT {GRAPH<http://workingGraph>
               {?vertex <temp:counts> ?newCount; 

            <temp:percentage> ?newPercentage; 
            <temp:difference> ?newDiff 
           }

       }
    WHERE{
         SELECT ?vertex (COUNT(?vertex) AS ?newCount) (COUNT (?vertex)/?total AS ?newPercentage) ?previousCount ?previousPercentage ?previousDiff (ABS(?newPercentage-?previousPercentage) AS ?newDiff)   

         {
        GRAPH<http://workingGraph1> {SELECT (COUNT(*) AS ?total) WHERE{?pathId ?step ?vertex}} 

        GRAPH<http://workingGraph1> {?pathId ?step ?vertex}
        GRAPH<http://workingGraph> 

            {?vertex <temp:difference> ?previousDiff; 
                     <temp:counts> ?previousCount; 
                 <temp:percentage> ?previousPercentage

            }
         }    
         GROUP BY ?vertex ?total ?previousCount ?previousPercentage ?previousDiff

         }
                       '''
    COMMAND_TO_GET_MAX_DIFFERENCE = ''' 
    SELECT (MAX(?diff) AS ?maxDifference)
    WHERE {GRAPH <http://workingGraph>
              {?vertex <temp:difference> ?diff}
      }                                     
                    '''
    COMMAND_TO_GET_RANDOM_WALK_RESULT = ''' 
    SELECT ?vertex ?percentage
    WHERE{GRAPH <http://workingGraph>
             {?vertex <temp:percentage> ?percentage}
     }
    ORDER BY DESC(?percentage)
                                        '''
    COMMAND_TO_GET_PATH_RANK_RESULT = '''               
    SELECT ?pathId (SUM(?per) AS ?score) (GROUP_CONCAT(?nodeId;SEPARATOR="->") AS ?nodes)
    WHERE{
          SELECT DISTINCT ?pathId ?nodeId ?per
      WHERE{
        GRAPH <http://workingGraph1>{?pathId ?stepId ?nodeId}
        GRAPH<http://workingGraph>{?nodeId <temp:percentage> ?per.}
           }
     }
    GROUP BY ?pathId    
    ORDER BY DESC(?score)
                                  '''

    def __init__(self):
        GraphAlgorithmModule.GraphAlgorithm.__init__(self, 'Random Walk')
     

    def initialize(self, LENGTH_OF_PATH, NO_OF_THREADS, MAX_ITERATION):
        self.connection = FusekiLogin('ds', 'solarpy')
        self.LENGTH_OF_PATH = LENGTH_OF_PATH
        self.NO_OF_THREADS = NO_OF_THREADS
        self.MAX_ITERATION = MAX_ITERATION
        self.BUFFER = [10,10,10,10,10,10,10,10,10,10]
        results = self.connection.urika.query(self.name, RandomWalk.COMMAND_TO_RETRIEVE_NUMBER_OF_SOURCES, None, None, 'json', True) 
        self.numberOfOutVertices = int(results['results']['bindings'][0]['count']['value'])
        self.convergenceThreshold = 1.0 / float(self.numberOfOutVertices) / 10.0 
        print ('Maximum percentage change should be smaller than: ' + str(self.convergenceThreshold))
        self.connection.urika.update(self.name, RandomWalk.COMMAND_TO_FINALIZE_WORKING_GRAPH)
        self.connection.urika.update(self.name, RandomWalk.COMMAND_TO_INITIALIZE_WORKING_GRAPH)
        self.connection.urika.update(self.name, RandomWalk.COMMAND_TO_INITIALIZE_RESULTS)

    def process(self):
        iteration = 0
        while True:
            start = time.time()
            self.connection.urika.update(self.name, RandomWalk.COMMAND_TO_GET_START_NODES.format(self.NO_OF_THREADS))
            for stepNo in range(1, self.LENGTH_OF_PATH+1): 
                self.connection.urika.update(self.name, RandomWalk.COMMAND_TO_RANDOM_WALK.format(stepNo, stepNo-1))
                self.connection.urika.update(self.name, RandomWalk.COMMAND_TO_UPDATE_PATH_RANK)
            if self.isConverged():
                print "Converged!"
                break
            if self.MAX_ITERATION == iteration:
                print "Max Iteration Reached!"
                break    
            end = time.time()
            iteration = iteration + 1
            print('Iteration ' + str(iteration) + ': ' + str(end-start) + ' seconds')
            #print(str(iteration))
            #print(str(end-start))

    def isConverged(self):
        results = self.connection.urika.query(self.name, RandomWalk.COMMAND_TO_GET_MAX_DIFFERENCE, None, None, 'json', True)
        difference = float(results['results']['bindings'][0]['maxDifference']['value'])
        print ('Convergence Rate: ' + str(difference))
        del self.BUFFER[0]
        self.BUFFER.append(difference)
        #print self.BUFFER
        return max(self.BUFFER) < self.convergenceThreshold

   
    def finalize(self):
        self.randomWalk()
        self.pathRank()
        print "Done!"


    def randomWalk(self):
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()           
        results = self.connection.urika.query(self.name, RandomWalk.COMMAND_TO_GET_RANDOM_WALK_RESULT, None, None, 'json', True) 
        for result in results['results']['bindings']:
            writer.writeLine(result['vertex']['value'] + ': ' + result['percentage']['value'])
        writer.close()

    
    def pathRank(self):
        writer = FileWriterModule.FileWriter(self.name)
        writer.open()
        results = self.connection.urika.query(self.name,RandomWalk.COMMAND_TO_GET_PATH_RANK_RESULT, None, None, 'json', True)    
        noOfPaths = 0
        writer.writeLine('************Top 100 Paths:***********\n')
        for result in results['results']['bindings']:
            writer.writeLine('No:' + str(noOfPaths+1) + '  ' + 'pathId: ' + result['pathId']['value'] + ', score: ' + result['score']['value'] + '\n The path is: ' + result['nodes']['value'])
            writer.writeLine('****************************************')
            noOfPaths = noOfPaths + 1
            if noOfPaths >= 100: break
        writer.close()
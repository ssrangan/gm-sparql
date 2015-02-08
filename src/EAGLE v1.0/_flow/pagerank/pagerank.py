#-------------------------------------------------------------------------------
# Name:        PageRank (Undirected & Directed) for Urika/Fuseki
# Purpose:     Computing PageRank Scores of all Node in the Graph
#
# Author:      "Sukumar, Sreenivas R." "Matt(Sangkeun) Lee"
#
# Created:     08/19/2014
# Copyright:   Oak Ridge National Laboratory
#------------------------------------------------------------------------------
import os
from PLUS.PLUS import UrikaLogin
from PLUS.PLUS import FusekiLogin

debug = False

# SUPPORT FUNCTIONS FOR THE MAIN ALGORITHM


def check_if_converged_avg(conn,attribGraph, convergenceThreshold, numNodes):

    global debug

    sumDiffQuery = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "sumDiff.rq")).read() %(attribGraph, attribGraph, attribGraph)

    if debug == True:
        print sumDiffQuery

    sumDiff = float(conn.urika.query("pagerank", string = sumDiffQuery)['results']['bindings'][0]['sumDiff']['value'])

    if sumDiff < convergenceThreshold * numNodes:
        return True
    else:
        print "current avgDiff: "+ str(float(sumDiff)/float(numNodes))
        return False

def check_if_converged_max(conn, attribGraph, convergenceThreshold):

    global debug
    maxDiffQuery = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "maxDiff.rq")).read() %(attribGraph, attribGraph, attribGraph)

    if debug == True:
        print maxDiffQuery

    maxDiff = float(conn.urika.query("pagerank", string = maxDiffQuery)['results']['bindings'][0]['maxDiff']['value'])

    if maxDiff < convergenceThreshold:
        return True
    else:
        print "current maxDiff: "+ str(float(maxDiff))
        return False

def init_PR(conn, attribGraph, initPR, predicate):

    global debug
    initPRassign = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "assign1ovrN.ru")).read() %(attribGraph, attribGraph, initPR, predicate, predicate)
    conn.urika.update("pagerank", string = initPRassign)
    if debug == True:
        print initPRassign

def init_outdegrees(conn, directed, attribGraph):

    global debug

    if directed:
        initOutDegreeQuery_d = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "assignOutDegree_d.ru")).read()  %(attribGraph, attribGraph )
        if debug == True:
            print initOutDegreeQuery_d
        conn.urika.update("pagerank", string = initOutDegreeQuery_d)
    else:
        initOutDegreeQuery_ud = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "assignOutDegree_ud.ru")).read()  %(attribGraph, attribGraph)
        if debug == True:
            print initOutDegreeQuery_ud
        conn.urika.update("pagerank", string = initOutDegreeQuery_ud)

def update_newPR(conn, directed, attribGraph, dampingFactor, addTerm, predicate):

    global debug
    if directed:
        iterNewPR = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "iter_newPR_d.ru")).read() %(attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, str(dampingFactor), addTerm, predicate, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, str(dampingFactor), addTerm, predicate, attribGraph, attribGraph, predicate)
        if debug == True:
            print iterNewPR
        conn.urika.update("pagerank", string = iterNewPR)
    else:
        iterNewPR = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "iter_newPR_ud.ru")).read() %(attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, str(dampingFactor), addTerm, predicate, attribGraph, attribGraph, attribGraph, predicate, attribGraph, attribGraph, attribGraph)
        if debug == True:
            print iterNewPR
        conn.urika.update("pagerank", string = iterNewPR)

def update_PR(conn, directed, attribGraph):

    global debug
    if directed:
        iterUpdatePR = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "iter_updatePR.ru")).read() %(attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph)
        if debug == True:
            print iterUpdatePR
        conn.urika.update("pagerank", string = iterUpdatePR)
    else:
        iterUpdatePR = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "iter_updatePR.ru")).read() %(attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph)
        if debug == True:
            print iterUpdatePR
        conn.urika.update("pagerank", string = iterUpdatePR)

# MAIN ALGORITHM

def pagerank (conn, directed, predicate, IRIAttrib, limit, dampingFactor, convergenceThreshold, maxIterations, diffType):


    if limit[0]: limit = "LIMIT %s" %str(limit[1])
    else: limit = ""

    global debug

    print "\n* Computing PageRank ..."

##    print "-----------------------------------"
##    print "damping Factor = "+ str(dampingFactor)
##    print "convergenceThreshold = "+ str(convergenceThreshold)
##    print "maxIterations = "+ str(maxIterations)
##    print "predicate = "+ str(predicate)
##    print "diffType = "+ str(diffType)
##    print "directed = "+ str(directed)
##    print "-----------------------------------"

    #IRIAttrib = "urn:flow/PageRank"
    attribGraph = "%s/PRAttrib" %(IRIAttrib)

    # 1) Cleaning up the named graph

    dropAttribTable = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "dropAttribGraph.ru")).read() %(attribGraph)
    if debug == True:
            print dropAttribTable
    conn.urika.update("pagerank", string = dropAttribTable)

    # 2) Compute the number of nodes and the initial PageRank score

    numNodesQuery = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "initPR_N.rq")).read()
    numNodes = float(conn.urika.query("pagerank", string = numNodesQuery)['results']['bindings'][0]['nodeCount']['value'])
    addTerm = str((1-dampingFactor)/numNodes)
    initPR = str(1/numNodes)

    # 3) Init out degree for each node in named graph; creates a new edge in named graph /outDeg

    init_outdegrees(conn, directed, attribGraph)

    # 4) Assigns PR for each node in named graph (1/N);

    init_PR(conn, attribGraph, initPR, predicate)

    # 5) Calculate newPR (finds 1st page rank of each node based on the init PR)

    print("Iteration: 1")

    update_newPR(conn, directed, attribGraph, dampingFactor, addTerm, predicate)

    for iterations in range(1,maxIterations):

        if diffType == "maxDiff":
            if_converged = check_if_converged_max(conn, attribGraph, convergenceThreshold)
        elif diffType== "avgDiff":
            if_converged = check_if_converged_avg(conn, attribGraph, convergenceThreshold, numNodes)
        else:
            if_converged = check_if_converged_max(conn, attribGraph, convergenceThreshold)

        # Check if converged

        if if_converged == True:
            print ("Convergence Threshold Reached!")
            break

        else:

            # Power iteration

            print("Iteration: %d" %(iterations+1))
            update_PR(conn, directed, attribGraph)
            update_newPR(conn, directed, attribGraph, dampingFactor, addTerm, predicate)

    resultsQuery = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "results.rq")).read() %(attribGraph, attribGraph, limit)
    resultsPR = conn.urika.query("pagerank", resultsQuery, accept = "csv")
    return resultsPR

def main():
    #conn = UrikaLogin("tfy", raw_input("Enter Password >> "))
    conn = FusekiLogin("ds")
    pagerank(conn, "http://xmlns.com/foaf/0.1/knows", .85, .00001, 100, (False, 0), "avgDiff", True)

if __name__ == '__main__':
    main()

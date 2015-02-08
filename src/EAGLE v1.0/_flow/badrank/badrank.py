#-------------------------------------------------------------------------------
# Name:        badrank (Undirected & Directed) for Urika/Fuseki
# Purpose:     Computing badrank Scores of all Node in the Graph
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

    sumDiff = float(conn.urika.query("badrank", string = sumDiffQuery)['results']['bindings'][0]['sumDiff']['value'])

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

    maxDiff = float(conn.urika.query("badrank", string = maxDiffQuery)['results']['bindings'][0]['maxDiff']['value'])

    if maxDiff < convergenceThreshold:
        return True
    else:
        print "current maxDiff: "+ str(float(maxDiff))
        return False

def init_BR(conn, attribGraph, initBR, predicate):

    global debug
    initBRassign = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "assign1ovrN.ru")).read() %(attribGraph, attribGraph, initBR, predicate, predicate)
    conn.urika.update("badrank", string = initBRassign)
    if debug == True:
        print initBRassign

def init_outdegrees(conn, directed, attribGraph):

    global debug

    if directed:
        initOutDegreeQuery_d = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "assignInDegree_d.ru")).read()  %(attribGraph, attribGraph )
        if debug == True:
            print initOutDegreeQuery_d
        conn.urika.update("badrank", string = initOutDegreeQuery_d)
    else:
        initOutDegreeQuery_ud = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "assignTotDegree_ud.ru")).read()  %(attribGraph, attribGraph)
        if debug == True:
            print initOutDegreeQuery_ud
        conn.urika.update("badrank", string = initOutDegreeQuery_ud)

def update_newBR(conn, directed, attribGraph, dampingFactor, addTerm, predicate):

    global debug
    if directed:
        iterNewBR = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "iter_newBR_d.ru")).read() %(attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, str(dampingFactor), addTerm, predicate, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, str(dampingFactor), addTerm, predicate, attribGraph, attribGraph, predicate)
        if debug == True:
            print iterNewBR
        conn.urika.update("badrank", string = iterNewBR)
    else:
        iterNewBR = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "iter_newBR_ud.ru")).read() %(attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, str(dampingFactor), addTerm, predicate, attribGraph, attribGraph, attribGraph, predicate, attribGraph, attribGraph, attribGraph)
        if debug == True:
            print iterNewBR
        conn.urika.update("badrank", string = iterNewBR)

def update_BR(conn, directed, attribGraph):

    global debug
    if directed:
        iterUpdateBR = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "iter_updateBR.ru")).read() %(attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph)
        if debug == True:
            print iterUpdateBR
        conn.urika.update("badrank", string = iterUpdateBR)
    else:
        iterUpdateBR = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "iter_updateBR.ru")).read() %(attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph, attribGraph)
        if debug == True:
            print iterUpdateBR
        conn.urika.update("badrank", string = iterUpdateBR)

# MAIN ALGORITHM

def badrank (conn, directed, predicate, IRIAttrib, limit, dampingFactor, convergenceThreshold, maxIterations, diffType):


    if limit[0]: limit = "LIMIT %s" %str(limit[1])
    else: limit = ""

    global debug

    print "\n* Computing badrank ..."

##    print "-----------------------------------"
##    print "damping Factor = "+ str(dampingFactor)
##    print "convergenceThreshold = "+ str(convergenceThreshold)
##    print "maxIterations = "+ str(maxIterations)
##    print "predicate = "+ str(predicate)
##    print "diffType = "+ str(diffType)
##    print "directed = "+ str(directed)
##    print "-----------------------------------"

    #IRIAttrib = "urn:flow/badrank"
    attribGraph = "%s/BRAttrib" %(IRIAttrib)

    # 1) Cleaning up the named graph

    dropAttribTable = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "dropAttribGraph.ru")).read() %(attribGraph)
    if debug == True:
            print dropAttribTable
    conn.urika.update("badrank", string = dropAttribTable)

    # 2) Compute the number of nodes and the initial badrank score

    numNodesQuery = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "initBR_N.rq")).read()
    numNodes = float(conn.urika.query("badrank", string = numNodesQuery)['results']['bindings'][0]['nodeCount']['value'])
    addTerm = str((1-dampingFactor)/numNodes)
    initBR = str(1/numNodes)

    # 3) Init out degree for each node in named graph; creates a new edge in named graph /outDeg

    init_outdegrees(conn, directed, attribGraph)

    # 4) Assigns BR for each node in named graph (1/N);

    init_BR(conn, attribGraph, initBR, predicate)

    # 5) Calculate newBR (finds 1st page rank of each node based on the init BR)

    print("Iteration: 1")

    update_newBR(conn, directed, attribGraph, dampingFactor, addTerm, predicate)

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
            update_BR(conn, directed, attribGraph)
            update_newBR(conn, directed, attribGraph, dampingFactor, addTerm, predicate)

    resultsQuery = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "results.rq")).read() %(attribGraph, attribGraph, limit)
    resultsBR = conn.urika.query("badrank", resultsQuery, accept = "csv")
    return resultsBR

def main():
    #conn = UrikaLogin("tfy", raw_input("Enter Password >> "))
    conn = FusekiLogin("ds")
    badrank(conn, True, "http://xmlns.com/foaf/0.1/knows", "BadRank", (False, 0), .85, .00001, 100, "avgDiff")

if __name__ == '__main__':
    main()

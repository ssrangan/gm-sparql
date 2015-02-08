#-------------------------------------------------------------------------------
#TODO: ADD CLOSE STATEMENTs
#TODO: ADD Print File Successful messages
#TODO: ADD select edge for degree and pass custom labels from main and not runtime exec
#TODO: consistent input and passing of wildcards
#TODO: bigurl results
#TODO: specify output type
#-------------------------------------------------------------------------------
import sys
import time
import logging
from _centrality.degree import degreeDist, degreeDistLabel
from _pattern.triangle import triangleDist, triangleDistLabel
from _flow.pagerank.pagerank import pagerank
from _clustering.peerpressure.peerpressure import peerPressure
from _flow.badrank.badrank import badrank
#from _flow.trustrank.trustrank import trustrank
from PLUS.PLUS import FusekiLogin, UrikaLogin

def inputFetchCheck():
    flavor, fusekiDataSet, fusekiHost, user, password, urikaHost = None, None, None, None, None, None

    try: flavor = sys.argv[1].replace("-", "").lower()
    except: pass
    while True:
        if flavor == "u" or flavor == "urika":
            flavor = "u"
            break
        elif flavor == "f" or flavor == "fuseki":
            flavor = "f"
            break
        else:
            flavor = raw_input("Connection Type? [urika|fuseki] >> ")

    if flavor == "u":
        try:
            if sys.argv[2].replace("-","").lower() == "l": loginSwitch = True
        except: loginSwitch = False

        try:
            if sys.argv[5].replace("-","").lower() == "h": hostSwitch = True
        except: hostSwitch = False

        if loginSwitch:
            try: user = sys.argv[3]
            except:
                print "Authorization Required"
                user = raw_input("Username >> ")
            try: password = sys.argv[4]
            except: password = raw_input("Password >> ")
        else:
            print "Authorization Required"
            user = raw_input("Username >> ")
            password = raw_input("Password >> ")

        if hostSwitch:
            try: urikaHost = sys.argv[6]
            except: urikaHost = raw_input("Urika (Remote) Host >> ")
            urikaHost = _hostCheck(urikaHost)
        else:
            urikaHost = "https://apollo.ccs.ornl.gov/"

    elif flavor == "f":
        try:
            if sys.argv[2].replace("-", "").lower() == "d": datasetSwitch = True
        except: datasetSwitch = False

        try:
            if sys.argv[4].replace("-", "").lower() == "h": hostSwitch = True
        except: hostSwitch = False

        if datasetSwitch:
            try: fusekiDataSet = sys.argv[3]
            except: fusekiDataSet = raw_input("Fuseki Dataset >> ")
        if hostSwitch:
            try: fusekiHost = sys.argv[5]
            except : fusekiHost = raw_input("Fuseki Endpoint >> ")
            fusekiHost = _hostCheck(fusekiHost)

        if not fusekiDataSet: fusekiDataSet = "ds"
        if not fusekiHost:  fusekiHost = "http://localhost:3030/"



    return(flavor, user, password, fusekiDataSet, fusekiHost, urikaHost)

def outputType():
    text = (raw_input("Output Type [screen|file] >> ")).lower()
    while True:
        if text == "screen" or text == "s":
            return(False)
        elif text == "f" or text == "file":
            return(True)
        else:
            text = (raw_input("Output Type [screen|file] >> ")).lower()

def customLabels():
    customLabels = (raw_input("Custom Lables? [y|n] >> ")).lower()
    while True:
        if customLabels == "yes" or customLabels == "y":
            return(True)
        elif customLabels == "no" or customLabels == "n":
            return(False)
        else:
            customLabels = (raw_input("Custom Lables? [y|n] >> ")).lower()

def customLimit():
    custom = (raw_input("Limit Number of Returned Results? [y|n] >> ")).lower()
    while True:
        if custom == "yes" or custom == "y":
            input = (raw_input("Limit By >> ")).lower()
            while True:
                try:
                    return(True, int(input))
                except:
                    print "Enter an Integer!"
                    input = (raw_input("Limit By >> ")).lower()

        elif custom == "no" or custom == "n":
            return(False, 0)
        else:
            custom = (raw_input("Limit Number of Returned Results? [y|n] >> ")).lower()

def definePredicate(conn, default=False):
    query = open(r"_flow\getEdges.rq").read()
    if default == True:
        results = conn.urika.query("EAGLEv1/flow/pagerank/getEdges", string = query, accept = "json")
        return(results['results']['bindings'][0]['edges']['value'])
    else:
        results = conn.urika.query("EAGLEv1/flow/pagerank/getEdges", string = query, accept = "csv")

        print results.replace("edges", "\nPossible Edge/Predicate Choices: \n--------------------------------")
        predicate = raw_input("Enter a Predicate >> ")
        predicate = predicate.replace(">", "").replace("<", "")
        while True:
            if results.find(predicate) == -1:
                print ("Input did not validate! \n")
                print ("%s\n") %(predicate)
                cont = raw_input("Is Input Correct? [y|n] >> ")
                if cont.lower() == "yes" or cont.lower() == "y":
                    return(predicate)
                else:
                   predicate = raw_input("Enter Predicate >> ")
            else:
                return(predicate)

def directedGraph():
    custom= (raw_input("Directed Graph? [y|n] >> ")).lower()
    while True:
        if custom == "yes" or custom == "y":
            return(True)
        elif custom == "no" or custom == "n":
            return(False)
        else:
            custom = (raw_input("Directed Graph? [y|n] >> ")).lower()

def FlowCustomParameters(algorithm):
    if algorithm == "pagerank" or algorithm == "pr" : IRIAttrib = "urn:flow/pagerank"
    elif algorithm == "badrank" or algorithm == "br" : IRIAttrib = "urn:flow/badrank"
    elif algorithm == "trustrank" or algorithm == "tr" : IRIAttrib = "urn:flow/trustrank"
    dampingFactor, maxIterations, convergenceThreshold, diffType = .85, 100, .0001, "avgDiff"
    print "\n IRIAttrib = %s \n dampingFactor = %f \n maxIterations = %d \n convergenceThreshold = %f \n diffType = %s \n" %(IRIAttrib, dampingFactor, maxIterations, convergenceThreshold, diffType)
    accept = raw_input("Accept Defaults? [y|n] >> ")
    while True:
        if accept == "yes" or accept == "y":
            return(IRIAttrib, dampingFactor, maxIterations, convergenceThreshold, diffType)
        elif accept == "no" or accept == "n":
            IRIAttrib = raw_input("Enter Name of Pagerank Graph >> ")
            IRIAttrib = IRIAttrib.replace("<","").replace(">","")  #TODO: ADD ARE YOU SURE?
            dampingFactor = raw_input("Enter Damping Factor >> ")
            while True:
                try:
                    dampingFactor = float(dampingFactor)
                    break
                except:
                    print ("Enter a Floating Point!")
                    dampingFactor = raw_input("Enter Damping Factor >> ")

            maxIterations = raw_input("Enter Maximum Iterations >> ")
            while True:
                try:
                    maxIterations = int(maxIterations)
                    break
                except:
                    print ("Enter an Integer!")
                    maxIterations = raw_input("Enter Maximum Iterations >> ")

            convergenceThreshold = raw_input("Enter Convergence Threshold >> ")
            while True:
                try:
                    convergenceThreshold = float(convergenceThreshold)
                    break
                except:
                    print ("Enter a Floating Point!")
                    convergenceThreshold = raw_input("Enter Convergence Threshold >> ")

            diffType = raw_input("Enter Type of Convergence Threshold Algorithm [avgDiff|maxDiff] >> ")
            while True:
                try: diffType = diffType.lower()
                except: diffType = raw_input("Enter Type of Convergence Threshold Algorithm [avgDiff|maxDiff] >> ")
                if diffType == "avgdiff" or diffType == "a":
                    diffType = "avgDiff"
                    break
                elif diffType == "maxdiff" or diffType == "m":
                    diffType = "maxDiff"
                    break
                else:
                    diffType = raw_input("Enter Type of Convergence Threshold Algorithm [avgDiff|maxDiff] >> ")


            return(IRIAttrib, dampingFactor, maxIterations, convergenceThreshold, diffType)
        else:
            accept = raw_input("Accept Defaults? [y|n] >> ")
def defineObj():
    obj = raw_input("Enter Object >> ")
    if obj.find("?") != -1: #found variable
        obj = obj
    elif obj.find(":") != -1: #found urn
        if obj[0] != "<":
            obj = "<" +obj
        if obj[-1] != ">":
            obj = obj +">"
    else: #assigns object
        obj = obj.replace('"',"")
        obj = '"%s"' %obj
    return(obj)

def peerPressureInit():
    graph = "urn:clustering/peerpressure"
    maxIterations = 20
    percentThreshold = .01
    verticesThreshold = 4
    print "\n graph = %s \n maxIterations = %s \n percentThreshold = %s \n verticesThreshold = %s \n" %(graph, maxIterations, percentThreshold, verticesThreshold)
    accept = raw_input("Accept Defaults? [y|n] >> ")
    while True:
        if accept == "yes" or accept == "y":
            return(graph, maxIterations, verticesThreshold, percentThreshold)
        elif accept == "no" or accept == "n":
            graph = raw_input("Enter Name of Peer Pressuring Clusting Resulting Graph >> ")
            graph = graph.replace("<","").replace(">","")  #TODO: ADD ARE YOU SURE?

            maxIterations = raw_input("Enter Maximum Iterations >> ")
            while True:
                try:
                    maxIterations = int(maxIterations)
                    break
                except:
                    print ("Enter an Integer!")
                    maxIterations = raw_input("Enter Maximum Iterations >> ")

            percentThreshold = raw_input("Enter Percent Threshold (Decimal Form) >> ")
            while True:
                try:
                    percentThreshold = float(percentThreshold)
                    break
                except:
                    print ("Enter a Floating Point!")
                    dampingFactor = raw_input("Enter Percent Threshold (Decimal Form) >>  ")

            verticesThreshold = raw_input("Enter Vertices Threshold >> ")
            while True:
                try:
                    verticesThreshold = int(verticesThreshold)
                    break
                except:
                    print ("Enter a Integer!")
                    verticesThreshold = raw_input("Enter Vertices Threshold >> ")
            return(graph, maxIterations, verticesThreshold, percentThreshold)
        else:
            accept = raw_input("Accept Defaults? [y|n] >> ")

def _hostCheck(hostString):
    if hostString[-1] != "/":
        hostString = hostString +"/"
    if hostString.find("http://") == -1 and hostString.find("https://") == -1:
        print "Improper Host Format!"
        while True:
            ssl = raw_input("Secure Host [y|n] >> ")
            if ssl.lower() == "yes": ssl = "y"
            elif ssl.lower() == "no": ssl = "n"
            if ssl.lower() == "y" or ssl.lower() == "n":
                break
            else:
                ssl = raw_input("Secure Host [y|n] >> ")
        if ssl == "y":
            hostString = ("https://"+hostString)
        elif  ssl == "n":
            hostString = ("http://"+hostString)

    return(hostString)


def main():
    logging.basicConfig(filename = "rdflib.log", mode = "w") #used by RDFLib (Specifically, rdflib.term)
    flavor, user, password, fusekiDataSet, fusekiHost, urikaHost = inputFetchCheck()
    if flavor == "f":
        conn = FusekiLogin(fusekiDataSet, host = fusekiHost)
    elif flavor == "u":
        conn = UrikaLogin(user, password, host = urikaHost)
    print "Welcome to EAGLE (EAGLE is a Algorithmic Graph Library for Exploratory-analysis)"
    while True:
        customInput = raw_input("EAGLE >> ")
        customInput = customInput.lower().replace("-", "").split(" ")

        try:
            if customInput[1] == "c" : customA = True
        except: customA = False

        try: algorithm = customInput[0].lower()
        except: pass
        while True:
            if algorithm == "degree" or algorithm == "dg":
                if customA == True:
                    textBool = outputType()
                    limit = customLimit()
                    labelsBool = customLabels()

                    if labelsBool:
                        predicate = definePredicate(conn)
                        obj = defineObj()
                        results = degreeDistLabel(conn, limit, predicate, obj)
                    else:
                        results = degreeDist(conn,limit)

                    if textBool:
                        filename = "degreeResults%s" %(str(int(time.time())))
                        f = open(filename, "w")
                        f.write(results.replace("\n", ""))
                        f.close()
                        print ("File successfully written to: %s" %filename)
                    else:
                        print results
                else:
                    limit = (True, 10)
                    print degreeDist(conn, limit)
                break
            elif algorithm == "pagerank" or algorithm == "pr" or algorithm == "badrank" or algorithm == "br" or algorithm == "trustrank" or algorithm == "tr":
                if customA == True:
                    textBool = outputType()
                    limit = customLimit()
                    predicate = definePredicate(conn)
                    directed = directedGraph()
                    IRIAttrib, dampingFactor, maxIterations, convergenceThreshold, diffType = FlowCustomParameters(algorithm)

                    if textBool:
                        if algorithm == "badrank" or algorithm == "br":
                            results = badrank(conn, directed, predicate, IRIAttrib, limit, dampingFactor, convergenceThreshold, maxIterations, diffType)
                            filename = "badRankResults%s" %(str(int(time.time())))
                            f = open(filename, "w")
                        elif algorithm == "pagerank" or algorithm == "pr":
                            results = pagerank(conn, directed, predicate, IRIAttrib, limit, dampingFactor, convergenceThreshold, maxIterations, diffType)
                            filename = "pageRankResults%s" %(str(int(time.time())))
                            f = open(filename, "w")
                        elif algorithm == "trustrank" or algorithm == "tr":
                            pass
                        f.write(results.replace("\n", ""))
                        print ("File successfully written to: %s" %filename)
                        f.close()
                    else:
                        if algorithm == "badrank" or algorithm == "br": results = badrank(conn, directed, predicate, IRIAttrib, limit, dampingFactor, convergenceThreshold, maxIterations, diffType)
                        elif algorithm == "pagerank" or algorithm == "pr": results = pagerank(conn, directed, predicate, IRIAttrib, limit, dampingFactor, convergenceThreshold, maxIterations, diffType)
                        elif algorithm == "trustrank" or algorithm == "tr": pass
                        print results
                else:
                    limit = (True, 10)
                    predicate = definePredicate(conn, default=True)
                    directed, dampingFactor, maxIterations, convergenceThreshold, diffType = True, .85, 100, .0001, "avgDiff"

                    if algorithm == "badrank" or algorithm == "br":
                        IRIAttrib = "urn:flow/badRank"
                        print badrank(conn, directed, predicate, IRIAttrib, limit, dampingFactor, convergenceThreshold, maxIterations, diffType)
                    elif algorithm == "pagerank" or algorithm == "pr":
                        IRIAttrib = "urn:flow/PageRank"
                        print pagerank(conn, directed, predicate, IRIAttrib, limit, dampingFactor, convergenceThreshold, maxIterations, diffType)
                    elif algorithm == "trustrank" or algorithm == "tr": pass
                break
            elif algorithm == "triangle" or algorithm == "triangles" or algorithm == "ta":
                if customA == True:
                    textBool = outputType()
                    limit = customLimit()
                    labelsBool = customLabels()

                    if labelsBool:
                        predicate = definePredicate(conn)
                        obj = defineObj()
                        results = triangleDistLabel(conn, limit, predicate, obj)
                    else:
                        results = triangleDist(conn,limit)

                    if textBool:
                        filename = "triangleResults%s" %(str(int(time.time())))
                        f = open(filename, "w")
                        f.write(results.replace("\n", ""))
                        f.close()
                        print ("File successfully written to: %s" %filename)
                    else:
                        print results
                else:
                    limit = (True, 10)
                    print triangleDist(conn, limit)
                break
            elif algorithm == "closeness" or algorithm == "cl":
                break
            elif algorithm == "betweeness" or algorithm == "bw":
                break
            elif algorithm == "peerpressure" or algorithm == "pp":
                if customA == True:
                    textBool = outputType()
                    limit = customLimit()
                    predicate = definePredicate(conn)
                    graph, maxIterations, verticesThreshold, percentThreshold = peerPressureInit()
                    results = peerPressure(conn, graph, limit, predicate, maxIterations, percentThreshold, verticesThreshold)
                    if textBool:
                        filename = "peerPressureResults%s" %(str(int(time.time())))
                        f = open(filename, "w")
                        f.write(results.replace("\n", ""))
                        f.close()
                        print ("File successfully written to: %s" %filename)
                    else:
                        print results
                else:
                    graph = "urn:clustering/peerpressure"
                    limit = (True, 10)
                    predicate = definePredicate(conn, default = True)
                    maxIterations = 20
                    percentThreshold = .01
                    verticesThreshold = 4
                    print peerPressure(conn, graph, limit, predicate, maxIterations, percentThreshold, verticesThreshold)
                break
            elif algorithm == "rankclus" or algorithm == "rc":
                break
            elif algorithm == "ngons" or algorithm == "ng":
                break
            elif algorithm == "snowball" or algorithm == "sb":
                break
            elif algorithm == "subdue" or algorithm == "sd":
                break
            elif algorithm == "collaborativefiltering" or algorithm == "cf":
                break
            elif algorithm == "fusionpower" or algorithm == "fp":
                break
            elif algorithm == "shortestpath" or algorithm == "sp":
                break
            elif algorithm == "beliefpropagation" or algorithm == "bp":
                break
            elif algorithm == "beliefdiffusion" or algorithm == "bd":
                break
            elif algorithm == "discovery" or algorithm == "dc":
                break
            elif algorithm == "help":
                break
            elif algorithm == "exit":
                exit()
            else:
                print "Algorithm not understood, Enter 'help' for more information"
                algorithm = raw_input("EAGLE >> ")


if __name__ == '__main__':
    main()

#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      tfy
#
# Created:     09/07/2014
# Copyright:   (c) tfy 2014
# Licence:     <your licence>
#-------------------------------------------------------------------------------
import os
# Degree of each node within graph (Number of occurences within the graph)
def degreeDist(conn, limit):
    func = "EAGLEv1/degree/degreeDist"
    if limit[0]: limit = "LIMIT %s" %str(limit[1])
    else: limit = ""
    query = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "degreeDist.rq")).read() %(limit)
    return(conn.urika.query(func, string = query, accept = "csv"))

# Degree based on named label
def degreeDistLabel(conn, limit, p, o):
    func = "EAGLEv1/degree/degreeDistLabel"
    if limit[0]: limit = "LIMIT %s" %str(limit[1])
    else: limit = ""
    query = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "degreeDistLabel.rq")).read() %(p,p,p,o,limit)
    return(conn.urika.query(func, string = query, accept = "csv"))

def main():
    pass

if __name__ == '__main__':
    main()

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

def triangleDist(conn, limit):
    func = "EAGLEv1/triangle/triangleDist"
    if limit[0]: limit = "LIMIT %s" %str(limit[1])
    else: limit = ""
    query = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "triangleDist.rq")).read() %(limit)
    return(conn.urika.query(func, string = query, accept = "csv"))

def triangleDistLabel(conn, limit, p, o):
    func = "EAGLEv1/triangle/triangleDistLabel"
    if limit[0]: limit = "LIMIT %s" %str(limit[1])
    else: limit = ""
    query = open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "triangleDistLabel.rq")).read() %(p,o,limit)
    return(conn.urika.query(func, string = query, accept = "csv"))



def main():
    pass

if __name__ == '__main__':
    main()

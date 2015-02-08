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

class summaryMetrics(object):
    def countOntologyRatio(self, conn):
        func = "EAGLEv1/summetric/countOntologyRatio"
        query = conn.urika.blockt(__file__,"_summetric/countOntologyRatio.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def commonHetero(self, conn):
        func = "EAGLEv1/summetric/commonHetero"
        query = conn.urika.blockt(__file__,"_summetric/commonHetero.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def commonImplicitClass(self, conn):
        func = "EAGLEv1/summetric/commonImplicitClass"
        query = conn.urika.blockt(__file__,"_summetric/commonImplicitClass.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def commonNamespaceLinks(self, conn):
        func = "EAGLEv1/summetric/commonNamespaceLinks"
        query = conn.urika.blockt(__file__,"_summetric/commonNamespaceLinks.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def countIsoBlanks(self, conn):
        func = "EAGLEv1/summetric/countIsoBlanks"
        query = conn.urika.blockt(__file__,"_summetric/countIsoBlanks.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def commonImplicitProperty(self, conn):
        func = "EAGLEv1/summetric/commonImplicitProperty"
        query = conn.urika.blockt(__file__,"_summetric/commonImplicitProperty.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def countTypedSubjects(self, conn):
        func = "EAGLEv1/summetric/countTypedSubjects"
        query = conn.urika.blockt(__file__,"_summetric/countTypedSubjects.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def countPredVoc(self, conn):
        func = "EAGLEv1/summetric/countPredVoc"
        query = conn.urika.blockt(__file__,"_summetric/countPredVoc.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def histoClassProperty(self, conn):
        func = "EAGLEv1/summetric/histoClassProperty"
        query = conn.urika.blockt(__file__,"_summetric/histoClassProperty.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def histoClassCount(self, conn):
        func = "EAGLEv1/summetric/histoClassCount"
        query = conn.urika.blockt(__file__,"_summetric/histoClassCount.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def histoSubjProperty(self, conn):
        func = "EAGLEv1/summetric/histoSubjProperty"
        query = conn.urika.blockt(__file__,"_summetric/histoSubjProperty.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def commonExplicitClass(self, conn):
        func = "EAGLEv1/summetric/commonExplicitClass"
        query = conn.urika.blockt(__file__,"_summetric/commonExplicitClass.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def histoDistinctPropertyCount(self, conn):
        func = "EAGLEv1/summetric/histoDistinctPropertyCount"
        query = conn.urika.blockt(__file__,"_summetric/histoDistinctPropertyCount.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def histoPropertyCount(self, conn):
        func = "EAGLEv1/summetric/histoPropertyCount"
        query = conn.urika.blockt(__file__,"_summetric/histoPropertyCount.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def commonExplicitProperty(self, conn):
        func = "EAGLEv1/summetric/commonExplicitProperty"
        query = conn.urika.blockt(__file__,"_summetric/commonExplicitProperty.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def commonTriplesSubject(self, conn):
        func = "EAGLEv1/summetric/commonTriplesSubject"
        query = conn.urika.blockt(__file__,"_summetric/commonTriplesSubject.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def countTripleSubjClass(self, conn):
        func = "EAGLEv1/summetric/countTripleSubjClass"
        query = conn.urika.blockt(__file__,"_summetric/countTripleSubjClass.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def countBlank(self, conn):
        func = "EAGLEv1/summetric/countBlank"
        query = conn.urika.blockt(__file__,"_summetric/countBlank.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def histoDistinctObjectProperty(self, conn):
        func = "EAGLEv1/summetric/histoDistinctObjectProperty"
        query = conn.urika.blockt(__file__,"_summetric/histoDistinctObjectProperty.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def histoDistinctPropertySubject(self, conn):
        func = "EAGLEv1/summetric/histoDistinctPropertySubject"
        query = conn.urika.blockt(__file__,"_summetric/histoDistinctPropertySubject.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def histoDistinctSubjectProperty(self, conn):
        func = "EAGLEv1/summetric/histoDistinctSubjectProperty"
        query = conn.urika.blockt(__file__,"_summetric/histoDistinctSubjectProperty.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def histoDistinctPropertyObject(self, conn):
        func = "EAGLEv1/summetric/histoDistinctPropertyObject"
        query = conn.urika.blockt(__file__,"_summetric/histoDistinctPropertyObject.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def countOwlSameAs(self, conn):
        func = "EAGLEv1/summetric/countOwlSameAs"
        query = conn.urika.blockt(__file__,"_summetric/countOwlSameAs.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def disinctEdges(self, conn):
        func = "EAGLEv1/summetric/disinctEdges"
        query = conn.urika.blockt(__file__,"_summetric/disinctEdges.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def countTriples(self, conn):
        func = "EAGLEv1/summetric/countTriples"
        query = conn.urika.blockt(__file__,"_summetric/countTriples.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def degreeAll(self, conn):
        func = "EAGLEv1/summetric/degreeAll"
        query = conn.urika.blockt(__file__,"_summetric/degreeAll.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def degreeIn(self, conn):
        func = "EAGLEv1/summetric/degreeIn"
        query = conn.urika.blockt(__file__,"_summetric/inDegree.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def degreeOut(self, conn):
        func = "EAGLEv1/summetric/degreeOut"
        query = conn.urika.blockt(__file__,"_summetric/outDegree.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def degreeMax(self, conn):
        func = "EAGLEv1/summetric/degreeMax"
        query = conn.urika.blockt(__file__,"_summetric/maxDegree.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

    def degreeNodeType(self, conn):
        func = "EAGLEv1/summetric/degreeNodeType"
        query = conn.urika.blockt(__file__,"_summetric/degreeNodeType.rq")
        return(conn.urika.query(func, file = query, accept = "csv"))

def main():
    pass

if __name__ == '__main__':
    main()

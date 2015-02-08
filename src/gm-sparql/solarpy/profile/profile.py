# Copyright 2013-2014 YarcData LLC, a Cray Company. All Rights Reserved.
#
# Run INSERT scripts to profile a database.
#

def profile( urika, ngraph, memfree = 0 ):
    """Inserts DB profile data into ngraph (drops first if it exists).
    """
    func = "profile"
    profileq = urika.blockt( __file__, "profile.rq" )
    urika.update( func, profileq, "INIT", ngraph = ngraph, memfree = memfree )
    def run( block, node ):
        urika.update( func, profileq, block, ngraph = ngraph, memfree = memfree )
        urika.update( func, profileq, "COMPLETED", ngraph = ngraph, node = node )
    run( "NGRAPH_COUNTS", "profile:namedGraphCounts" )
    run( "TYPE_COUNTS", "profile:typeCounts" )
    run( "PRED_COUNTS", "profile:predCounts" )
    # TODO: convert to inserts
    # urika.query( func, self.commonrq, "PRED_STATS_NUMBER", accept = "csv", range = "xsd:integer" )
    # urika.query( func, self.commonrq, "PRED_STATS_NAN", accept = "csv", range = "rdf:Resource" )
    # urika.query( func, self.commonrq, "PRED_STATS_NAN", accept = "csv", range = "xsd:string" )
    # urika.query( func, self.commonrq, "PRED_STATS_NAN", accept = "csv", range = "rdfs:Literal" )
    # TODO: per pred, collect s and o types, count and ratio to pred size. See George's slides about mining/profiling
    urika.update( func, profileq, "COMPLETED", ngraph = ngraph, node = ngraph )

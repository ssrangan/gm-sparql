# Copyright 2013 YarcData LLC, a Cray Company. All Rights Reserved.

import os
import time
from datetime import datetime
from StringIO import StringIO
import traceback
import logging
import decimal

from rdflib import Graph, URIRef, RDF, RDFS, XSD, Namespace, Literal
from rdflib.term import Identifier

from Graphy import Graphy, GraphEntity
from BlockTemplate import BlockTemplate

log = None

class TrackingEntity ( GraphEntity ):

    def __init__( self, trackingcontext, graph, uri, ns, log_level = logging.DEBUG ):
        GraphEntity.__init__( self, graph, uri, ns )
        self.trackingcontext = trackingcontext
        self.log_level = log_level

    def entrystr( self, uri, msg, params ):
        out = StringIO()
        out.write( self.uriseg( uri ) )
        if ( msg is not None ):
            out.write( " " )
            out.write( msg )
        for i, key in enumerate( sorted( params.keys() ) ):
            if ( key == "ancestor" ):
                continue
            val = params[ key ]
            if ( val is None ):
                continue
            if ( i == 0 ):
                out.write( ": " )
            elif ( i > 0 ):
                out.write( " " )
            out.write( key )
            out.write( "=" )
            out.write( str( val ) )
        return out.getvalue()

    def addict( self, params ):
        for k in params.keys():
            v = params[ k ]
            if ( v is not None ):
                self.add( self.coerceuri( k ), self.coerceterm( v ) )

    def start( self, typename, verbose = True, **params ):
        self.start = time.time()
        self.add( RDF.type , self.coerceuri( typename ) )
        self.add( self.ns.start , Literal( datetime.utcnow() ) )
        self.addict( params )
        if verbose:
            log.log( self.log_level, self.entrystr( self.uri, None, params ) )
        else:
            log.log( logging.DEBUG, self.entrystr( self.uri, None, params ) )
        return self

    def elapsed( self ):
        return int( round( time.time() - self.start ) )

    def sec( self ):
        seconds = self.elapsed()
        if ( seconds is None ):
            return "t=None"
        elif ( seconds < 120 ):
            return "t=%ds" % seconds
        else:
            return "t=%dm" % ( seconds / 60 )

    def stop( self, **params ):
        sec = self.elapsed()
        self.add( self.ns.seconds , Literal( sec ) )
        self.add( self.ns.stop , Literal( datetime.utcnow() ) )
        log.log( self.log_level, self.entrystr( self.uri, "t=%ss" % sec, params ) )
        self.addict( params )
        return sec

    def error( self, msg, **params ):
        sec = self.elapsed()
        self.add( self.ns.error , Literal( msg ) )
        self.add( self.ns.seconds , Literal( sec ) )
        self.add( self.ns.stop , Literal( datetime.utcnow() ) )
        log.error( self.entrystr( self.uri, "t=%ss %s" % ( sec, msg ), params ) )
        self.addict( params )
        return sec

    def basefilename( self, base ):
        tracking = self.trackingcontext.tracking
        if ( tracking.options.resultsDir == None ):
            return
        c = self.trackingcontext.count
        self.trackingcontext.count += 1
        base = base.replace("<","").replace(">","").replace("/","_").replace(":","").replace("%","")
        base = "%05d%d_%s" % ( tracking.count, c, base )
        return base

    def results_filename( self, base, ext ):
        tracking = self.trackingcontext.tracking
        if ( tracking.options.resultsDir == None ):
            return
        filename = self.basefilename( base )
        # TODO filename = "%s/res%s_%s_share%s.%s" % ( tracking.options.resultsDir, filename, tracking.options.metadata, tracking.options.share, ext )
        filename = "%s/res%s.%s" % ( tracking.options.resultsDir, filename, ext )
        self.add( self.ns.results_filename , Literal( filename ) )
        return filename

    def logfile( self, base, data ):
        tracking = self.trackingcontext.tracking
        if ( not hasattr( tracking.options, "resultsDir" ) or tracking.options.resultsDir == None or tracking.options.logdir == None ):
            log.info( "Tracking.logfile no option for resultsDir, so can not save to file: %s: %s", base, data )
            return
        filename = tracking.options.logdir + "/trk" + self.basefilename( base )
        if ( not tracking.options.dryrun ):
            with open( filename, 'w' ) as f:
                if ( data is not None ):
                    f.write( data )
        log.debug( "logfile [%s]", filename )

class TrackingContext ( GraphEntity ):

    def __init__( self, tracking, graph, subject, ns, log_level ):
        GraphEntity.__init__( self, graph, subject, ns )
        self.tracking = tracking
        self.log_level = log_level
        self.count = 0

    def context( self, ctx , log_level = logging.DEBUG ):
        self.graph.add( ( ctx, RDF.type, self.ns.Context ) )
        self.graph.add( ( ctx, RDFS.label, Literal( self.uriseg( ctx ) ) ) )
        self.graph.add( ( ctx, self.ns.context, self.uri ) )
        return TrackingContext( self.tracking, self.graph, ctx, self.ns, log_level )

    def start( self, typename, trkid = None, verbose = True, **params ):
        self.graph.add( ( self.coerceuri( typename ), RDFS.label, Literal( self.uriseg( typename ) ) ) )
        if trkid is None:
            trkid = self.tracking.count
            self.tracking.count += 1
        self.count = 0
        # ns = Namespace( self.ns[ typename + "/" ] )
        s = TrackingEntity( self, self.graph, URIRef( self.ns[ typename + "_" + str( trkid ) ] ), self.ns, self.log_level )
        s.add( self.ns.context, self.uri )
        return s.start( typename, verbose, **params )

    def entries( self, typename, **params ):
        return self.tracking.entries( typename, **params )

class Tracking ( Graphy ):

    def __init__( self, options, m_log, m_graph = None ):
        global log
        Graphy.__init__( self,
                         m_graph if m_graph is not None else Graph(),
                         Namespace( "http://urika/track/" ) )
        log = m_log
        self.options = options
        self.count = 0

        self.graph.add( ( self.ns.Context, RDFS.label, Literal( "Context" ) ) )
        self.graph.add( ( self.ns.context, RDFS.label, Literal( "context" ) ) )

    def trackingentity( self, subj, ns = None ):
        return TrackingEntity( None, self.graph, subj, ns if ns is not None else self.ns )

    def started( self, subj ):
        return self.graph.value( subj, self.ns.start )

    def subjects_sorted_by_start( self ):
        return sorted( self.graph.subjects( self.ns.start ),
                       key = lambda x: self.started( x ) )

    def stats_csv( self, query_block ):
        out = StringIO()
        blocks = BlockTemplate( __file__, "Tracking.rq" )
        text = blocks.build( query_block )
        try:
            hdr = [ "seconds", "count", "avg", "min", "max", "dbmem_max", "context", "label" ]
            for i, h in enumerate( hdr ):
                if ( i > 0 ):
                    out.write("\t")
                out.write( h )
            out.write("\n")
            results = self.graph.query( text )
            for row in results:
                for i, h in enumerate( hdr ):
                    if ( i > 0 ):
                        out.write("\t")
                    v = row[ h ]
                    if ( v is not None ):
                        p = v.toPython()
                        if ( type( p ) == decimal.Decimal ):
                            out.write( int( round( p ) ) )
                        else:
                            out.write( p )
                out.write("\n")
        except:
            log.debug( "Tracking stats_csv %s query:\n%s", query_block, text )
            log.warning( "Tracking: %s", traceback.format_exc( None ) )
        return out.getvalue()

    def stats( self, trk ):
        log.info( "Tracking: STATS_HIGH:\n%s", self.stats_csv( "STATS_HIGH" ) )
        with open( trk.results_filename( "tracking_stats", "csv" ), "wb" ) as out:
            out.write( self.stats_csv( "STATS_ALL" ) )
        trk.logfile( "tracking.ttl", self.graph.serialize( format = 'turtle' ) )

    def ntriples( self ):
        # log.debug( "Tracking: %s", self.graph.serialize( format = 'turtle' ) )
        return self.graph.serialize( format = 'nt' )

    def entries( self, typename, **params ):
        r = []
        for s in self.graph.subjects( RDF.type, self.coerceuri( typename ) ):
            if ( len( params ) == 0 ):
                r.append( self.trackingentity( s ) )
            for k in params.keys():
                p = self.coerceuri( k )
                o = self.coerceterm( params[ k ] )
                if ( p is None or o is None ):
                    continue
                if ( ( s, p, o ) in self.graph ):
                    r.append( self.trackingentity( s ) )
        return r

    def context( self, ctx, log_level = logging.DEBUG ):
        self.graph.add( ( ctx, RDF.type, self.ns.Context ) )
        self.graph.add( ( ctx, RDFS.label, Literal( self.uriseg( ctx ) ) ) )
        return TrackingContext( self, self.graph, ctx, self.ns, log_level )

# Copyright 2013 YarcData LLC, a Cray Company. All Rights Reserved.

from StringIO import StringIO

from rdflib import Graph, URIRef, RDFS, Literal
from rdflib.term import Identifier

class Graphy:

    def __init__( self, graph, ns ):
        self.graph = graph
        self.ns = ns

    def uriseg( self, uri ):
        uri = str( uri )
        a = uri.rfind( "/" )
        b = uri.rfind( "#" )
        i = max( a, b )
        if ( i >= 0 ):
            return uri[ i + 1 : ]
        return uri

    def coerceuri( self, x ):
        if ( x is None ):
            return None
        if ( type( x ) == str ):
            return self.ns[ x ]
        if ( type( x ) == unicode ):
            return self.ns[ x ]
        if ( isinstance( x, GraphEntity ) ):
            return x.uri
        if ( isinstance( x, Identifier ) ):
            return x

    def coerceterm( self, x ):
        if ( x is None ):
            return None
        if ( isinstance( x, GraphEntity ) ):
            return x.uri
        if ( isinstance( x, Identifier ) ):
            return x
        return Literal( x )

class GraphEntity ( Graphy ):

    def __init__( self, graph, uri, ns ):
        Graphy.__init__( self, graph, ns )
        self.uri = uri

    def value( self, pred, default = None ):
        # if ( pred is not self.coerceuri( pred ) ):
        #     p = self.coerceuri( pred )
        #     print "AHHHH value coerceuri", pred, type( pred ), type( pred ) == unicode, type( pred ) == str, isinstance( pred, Identifier ), p, type( p )
        # return self.graph.value( self.uri, pred, default = default )
        return self.graph.value( self.uri, self.coerceuri( pred ), default = default )

    def val( self, pred ):
        v = self.value( pred )
        return v.toPython() if v is not None else None

    def valp( self, pred_name ):
        return self.val( self.ns[ pred_name ] )

    def label( self, default = '' ):
        lit = self.graph.label( self.uri )
        if ( lit ):
            return lit.toPython()
        else:
            return default

    def comment( self, default = '' ):
        lit = self.value( RDFS.comment )
        if ( lit ):
            return lit.toPython()
        else:
            return default

    def objects( self, pred ):
        return self.graph.objects( self.uri, self.coerceuri( pred ) )

    def add( self, pred, obj ):
        self.graph.add( ( self.uri, self.coerceuri( pred ), self.coerceterm( obj ) ) )

    def addlit( self, pred, obj ):
        self.graph.add( ( self.uri, self.coerceuri( pred ), Literal( obj ) ) )

    def set( self, pred, obj ):
        self.graph.set( ( self.uri, self.coerceuri( pred ), obj ) )

    def strln( self ):
        out = StringIO()
        out.write( self.uriseg( self.uri ) )
        out.write( ": " )
        for p in sorted( self.graph.predicates( self.uri ) ):
            pwritten = False
            for o in self.graph.objects( self.uri, p ):
                if ( isinstance( o, Literal ) ):
                    if ( not pwritten ):
                        out.write( self.uriseg( p ) )
                        out.write( "=" )
                        pwritten = True
                    out.write( str( o ) )
                    out.write( " " )
        # out.write( "\n" )
        return out.getvalue()

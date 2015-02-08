# Copyright 2013 YarcData LLC, a Cray Company. All Rights Reserved.

import datetime

baseURI = "http://urika/ivs/v1/"

#
# RDF
#

pre = {
    'rdf':  'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
    'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
    'xsd':  'http://www.w3.org/2001/XMLSchema#',
    'owl':  'http://www.w3.org/2002/07/owl#',
    'dc':   'http://purl.org/dc/elements/1.1/',
    None:     baseURI }

rdftype = '<' + pre['rdf'] + 'type' + '>'
a = rdftype
label = '<' + pre['rdfs'] + 'label' + '>'
comment = '<' + pre['rdfs'] + 'comment' + '>'

def xsdLit(i, xsdType):
    return '"' + str(i) + '"^^<' + pre['xsd'] + xsdType + '>'

def xsdInt(i):
    return xsdLit(i, 'int')

def xsd_long(i):
    return xsdLit(i, 'long')

def xsdString(s):
    return xsdLit(i, 'string')

def lit(s):
    return '"' + s + '"'

rdbToXsdTypes = {'numeric': 'decimal',
                 'date': 'date',
                 'char': 'string',
                 'character': 'string',
                 'bpchar': 'string',
                 'varchar': 'string',
                 'timestamp': 'dateTime',
                 'integer': 'int',
                 'boolean': 'boolean',
                 'bigint': 'long',
                 'smallint': 'int'}

pyToXsdTypes = { str: 'string',
                 unicode: 'string',
                 int: 'integer',
                 bool: 'boolean',
                 long: 'long',
                 float: 'float',
                 datetime.datetime: 'dateTime' }

def parseTS(val):
    if (len(val) == 16):
        return datetime.strptime(val, "%Y-%m-%d %H:%M")
    else:
        return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")

def xsdTypeUri(columnType):
    return '<' + pre['xsd'] + rdbToXsdTypes[columnType] + '>'

def uri(prefix, name):
    return '<' + pre[prefix] + name + '>'

def typedLit(val, columnType):
    if ( columnType == "timestamp" ):
        val = parseTS(val).strftime("%Y-%m-%dT%H:%M:%S")
    if ( columnType == "boolean" ):
        if ( val == "1" ):
            val = "true"
        elif ( val == "0" ):
            val = "false"
    return '"' + str( val ) + '"^^' + xsdTypeUri(columnType)

def escapeUri(s):
    return str(s).replace(" ", "_").replace(":", "_")

def id(type, id):
    return "<" + baseURI + type + "/" + escapeUri(id) + ">"

def p(name):
    return "<" + baseURI + "p#" + name + ">"

def s(name):
    return "<" + baseURI + name + ">"

def myType(name):
    return "<" + baseURI + "t#" + name + ">"

def ln(s, out):
    out.write(s + "\n")

def trp(s, p, o, out):
    # out.write(s + " " + p + " " + o + " .\n")
    out.write(str(s) + " " + str(p) + " " + str(o) + " .\n")

class RdferEnt:

    def __init__( self, rdfer, uri, rdftype ):
        self.rdfer = rdfer
        self.uri = uri
        self.rdfer.type( self.uri, rdftype )

    def say( self, **params ):
        for key in params.keys():
            val = params[ key ]
            if val is None:
                pass
            elif val == "a":
                self.rdfer.type( self.uri, val )
            elif isinstance( val, RdferEnt ):
                self.rdfer.trp( self.uri, self.rdfer.base( key ), val.uri )
            else:
                self.rdfer.trplit( self.uri, self.rdfer.base( key ), val )

class Rdfer:

    def __init__( self, out, baseuri ):
        self.baseuri = baseuri
        self.out = out
        self.count = 0

    def rdf( self, s ):
        return "<" + pre['rdf'] + s + ">"

    def rdfs( self, s ):
        return "<" + pre['rdfs'] + s + ">"

    def xsd( self, s ):
        return "<" + pre['xsd'] + s + ">"

    def base( self, s ):
        return "<" + self.baseuri + s + ">"

    def resource( self, s, id ):
        return "<" + self.baseuri + s + "/" + id + ">"

    def uri( self, s ):
        return "<" + s + ">"

    def lit( self, val ):
        if ( val is None ):
            return
        else:
            valtype = type( val )
            if ( valtype is bool ):
                return '"' + str( val ).lower() + '"^^' + self.xsd( pyToXsdTypes[ valtype ] )
            elif ( valtype is datetime.datetime ):
                # TODO ms
                return '"' + val.strftime("%Y-%m-%dT%H:%M:%SZ") + '"^^' + self.xsd( pyToXsdTypes[ valtype ] )
            else:
                xsdtype = pyToXsdTypes.get( valtype )
                if xsdtype:
                    return '"' + str( val ) + '"^^' + self.xsd( xsdtype )
                else:
                    raise Exception( "unknown type: %s of %s" % ( valtype, val ) )
                    # return '"' + str( val ) + '"'

    def trp( self, s, p, o ):
        out = self.out
        out.write( s )
        out.write( " " )
        out.write( p )
        out.write( " " )
        out.write( o )
        out.write( " .\n" )
        self.count += 1
        return uri

    def trplit( self, uri, pred, val ):
        if ( val is not None ):
            self.trp( uri, pred, self.lit( val ) )
        return uri

    def type( self, uri, rdftype ):
        self.trp( uri, self.rdf( "type" ), rdftype )
        return uri

    def ent( self, uri, rdftype ):
        return RdferEnt( self, uri, rdftype )

    def label( self, uri, string ):
        self.trplit( uri, self.rdfs( "label" ), string )
        return uri

    def declare( self, uri, rdftype, label, comment = None ):
        self.type( uri, rdftype )
        self.label( uri, label )
        if ( comment is not None ):
            self.trplit( uri, self.rdfs( "comment" ), comment )
        return uri

    def quote_encode( self, lit ):
        return '"' + lit.replace('\\', '\\\\')\
            .replace('\n', '\\n')\
            .replace('"', '\\"')\
            .replace('\r', '\\r') + '"'

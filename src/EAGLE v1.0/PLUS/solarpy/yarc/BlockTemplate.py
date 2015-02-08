# Copyright 2013 YarcData LLC, a Cray Company. All Rights Reserved.

import os
import re
from string import Template
import StringIO
import types

# class BlockDef(object):
#     def __init__(self, filename, block, params):
#         self.filename = filename
#         self.block = block
#         self.params = params

depth = 0

def relfile( nearby, filename ):
    return os.path.join( os.path.split( nearby )[ 0 ], filename )

def doInclude( includeLines, indent, params, out ):
    for line in includeLines:
        if ( type( line ) == str ):
            ind = "" if (line.isspace() or len( line ) == 0) else indent
            # TODO: using Template is only partial support for INCLUDE args, should use build recursively
            line = Template( line ).safe_substitute( params )
            if (line.endswith('\n')):
                out.write( ind + line )
            else:
                out.write( ind + line + '\n' )
        elif ( type( line ) == list ):
            if line[0] == "INCLUDE":
                run_include( indent + line[1], line[2], dict( line[3].items() + params.items() ), line[4], out )
            elif line[0] == "FOREACH":
                run_foreach( indent + line[1], line[2], dict( line[3].items() + params.items() ), line[4], out )

def run_foreach( indent, name, args, blocks, out ):
    args = dict( args.items() )
    var = args.pop( 'var' )
    list_name = args.pop( 'list' )
    lst = args.pop( list_name , [] )
    for x in lst:
        args[ var ] = x
        run_include( indent, name, args, blocks, out )

def dict_brief_str( d ):
    brief = {}
    for k in d.keys():
        if ( k == "file" or k == "block" or k == "debug" ):
            continue
        v = str( d[ k ] )
        if ( len( v ) > 8 ):
            v = v[ 0 : 8 ]
        brief[ k ] = v
    return brief

def run_include( indent, name, params, blocks, out ):
    # print( "run_include", name, params )
    global depth
    if (depth > 6): raise Exception( "depth=%d %s" % ( depth, name ) )
    depth +=1
    if ( name in params ):
        val = params[ name ]
        if ( val in blocks ):
            block = blocks[ val ]
            args = dict( block[ 'args' ].items() + params.items() )
            lines = block[ 'lines' ][ : ]
            if params.get( "debug" ) != False:
                lines.insert( 0, "# %s %s\n" % ( block[ 'name' ], dict_brief_str( args ) ) )
            doInclude( lines, indent, args, out )
        elif ( val is not None ):
            doInclude( val.split('\n'), indent, params, out )
    elif ( name in blocks ):
        block = blocks[ name ]
        args = dict( block[ 'args' ].items() + params.items() )
        lines = block[ 'lines' ][ : ]
        if params.get( "debug" ) != False:
            lines.insert( 0, "# %s %s\n" % ( block[ 'name' ], dict_brief_str( args ) ) )
        # print( "doInclude", name, indent, args )
        doInclude( lines, indent, dict( args.items() + params.items() ), out )
    depth -=1

def load( filename ):
    blocks = {}
    with open( filename, 'r') as f:
        curr = None
        for line in f:
            match = re.match( r"(?P<indent> *)#\$ (?P<op>[a-zA-Z_]+) *(?P<name>[a-zA-Z0-9_\-]+) *(?P<rest>.*)", line )
            if ( match is None ):
                if ( not curr in blocks ):
                    blocks[ curr ] = { 'name': curr,
                                       'lines': [],
                                       'args': dict() }
                lines = blocks[ curr ][ 'lines' ]
                if ( line.strip() != "#" ):
                    lines.append( line )
            else:
                indent = match.group( 'indent' )
                op = match.group( 'op' )
                name = match.group( 'name' )
                rest = match.group( 'rest' )
                args = dict()
                if ( rest ):
                    argstrings = rest.split( " " )
                    args = dict( [ x.split( "=" ) for x in argstrings ] )
                if ( op == 'BLOCK' ):
                    curr = name
                    blocks[ curr ] = { 'name': curr,
                                       'lines': [],
                                       'args': args }
                elif ( op == 'INCLUDE' ):
                    blocks[ curr ][ 'lines' ].append( [ op, indent, name, args, blocks ] )
                elif ( op == 'FOREACH' ):
                    blocks[ curr ][ 'lines' ].append( [ op, indent, name, args, blocks ] )
    return blocks

class BlockTemplate:

    def __init__( self, nearby, filename ):
        self.name = filename
        self.blocks = load( os.path.join( os.path.split( nearby )[ 0 ], filename ) )

    # def debug( self ):
    #     for b in self.blocks:
    #         print "BLOCK", b
    #         for ln in self.blocks[ b ][ 'lines' ]:
    #             print "\t", ln

    def build( self, block, **params ):
        out = StringIO.StringIO()
        run_include( '', block, params, self.blocks, out )
        text = out.getvalue()
        out.close()
        return text

# def blocks(filename):
#     result = {}
#     with open(filename, 'r') as f:
#         curr = None
#         for line in f:
#             m = re.match(r" *#\$ ([a-zA-Z_]+) *([a-zA-Z_]+)", line)
#             if (m == None):
#                 if (not curr in result):
#                     result[ curr ] = []
#                 result[ curr ].append( line )
#             else:
#                 op = m.group(1)
#                 name = m.group(2)
#                 if (op == 'BLOCK'):
#                     curr = name
#     # print "%s" % result
#     r = {}
#     for k in result.keys():
#         r[ k ] = string.join( result[ k ], '')
#     return r

# def build2(filename, block, params):
#     bks = blocks(filename)
#     return Template( bks[ block ] ).substitute(params)
    

#
# Test
#

if __name__ == '__main__':
    # print build( file= "BlockTemplate-test.rq",
    #              block= "CONSTRUCT",
    #              o= '"OOO"' )
    # print build( file= "BlockTemplate-test.rq",
    #              block= "INCLUDE_ARG" )
    # print build( file= "BlockTemplate-test.rq",
    #              block= "BLOCK_ARG" )
    # print build( file= "BlockTemplate-test.rq",
    #              block= "BLOCK_ARG_OVERRIDE" )
    print build( file= "labelprop.rq",
                 block= "TEST_FIRST_MOST_POPULAR_TIE" )

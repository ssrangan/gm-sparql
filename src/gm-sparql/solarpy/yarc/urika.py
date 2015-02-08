#! /usr/bin/env python
#
# Copyright 2013-2014 YarcData LLC, a Cray Company. All Rights Reserved.
#
# Urika API for query, update, monitoring, and admin functions.
#

import sys
import os
from StringIO import StringIO
import logging
import traceback
import math
from datetime import datetime
import optparse
import re
import subprocess
import time
import requests

from BlockTemplate import BlockTemplate, relfile
from yarcutils import hbytes, LogExtra, process
from Tracking import Tracking

try:
	import solarpy.profile.profile
except:
	from ..profile import profile

def pyname():
    myname = sys.argv[ 0 ]
    a = myname.rfind( "/" )
    if ( a >= 0 ):
        myname = myname[ a + 1 : ]
    return myname

class DbMem:

    def __init__( self, max, used ):
        self.max = max
        self.used = used
        self.free = max - used

#
# Urika API for query, update, checkpoint, restart database, monitoring, and maybe more
#
# Login username/password is a Urika user, the same as GAM.
# An admin user is required to use the admin functions.
#
# Warning: the urika-admin REST API is not a supported nor documented Urika API.
#
class Urika( object ):

    @staticmethod
    def blockt( nearby, filename ):
        return BlockTemplate( nearby, filename )

    def __init__( self, options, log, log_level, track ):
        self.log_level = log_level
        self.track = track
        self.trackq = track.context( track.ns.urika_query, log_level )
        self.tracku = track.context( track.ns.urika_update, log_level )
        self.trackc = track.context( track.ns.urika_update_checkpoint, log_level )
        self.tracka = track.context( track.ns.urika_admin, logging.INFO )
        self.prologue = ""
        self.log = log
        self.options = options
        self.state = None
        self.session = None
        self.checkpoint_created = None
        self.dbmemmax = options.dbmemmax
        self.commonrq = Urika.blockt( __file__, "common.rq" )
        self.solarq = Urika.blockt( __file__, "solar.rq" )

        if options.flavor == "fuseki":
            self.queryUrl = "%squery" % options.url
            self.queryBigUrl = self.queryUrl
            self.updateUrl = "%supdate" % options.url
        elif options.flavor == "urika":
##            self.queryUrl = "%sdataset/sparql/%s/query" % ( options.url, options.db )
##            self.queryBigUrl = "%sdataset/sparql/%s/all" % ( options.url, options.db )
##            self.updateUrl = "%sdataset/sparql/%s/update" % ( options.url, options.db )
# These parameters are now set after login TFY, ORNL 7/23/2014
            self.queryUrl = None
            self.queryBigUrl = None
            self.updateUrl = None
            if options.admin:
                self.adminUrl =  "%surika-admin/api/" % options.url

    def skip_admin( self ):
        return not self.options.admin or self.options.flavor == "fuseki"

    def set_prologue( self, prologue_text ):
        self.prologue = prologue_text

    def logg( self, msg, *args, **kwargs ):
        self.log.log( self.log_level, msg, *args, **kwargs )

    def login( self ):
        if self.options.flavor == "fuseki":
            self.session = requests
            return
        func = "login"
        if self.options.dryhttp:
            return
        if self.session != None:
            self.session.close()
            self.session = None
        self.session = requests.Session()
        # page = "dologin.html"
        page = "dologinCmd.html" # Fa13
        url =  "%s%s" % ( self.options.url, page )
        # verify=False because Urika ssl cert is self-signed
        trk = self.track.start( func, url = url, username = self.options.username )
        response = self.session.post( url, verify=False, timeout= 60,
                                      data={ 'httpd_username': self.options.username,
                                             'httpd_password': self.options.password } )

        dbresponse = self.db_status()                                                                                       #
        self.options.db = dbresponse['name']                                                                                #
        self.queryUrl = "%sdataset/sparql/%s/query" % ( self.options.url, self.options.db )                                 #  ADDED TCB, ORNL 7/22/2014
        self.queryBigUrl = "%sdataset/sparql/%s/all" % ( self.options.url, self.options.db )                                #  This will automatically set the database to the currently running db.
        self.updateUrl = "%sdataset/sparql/%s/update" % ( self.options.url, self.options.db )                               #  Now, there is no need to know which db is currently running.

        if ( response.text.find("Current database" ) == -1
             and response.text.find("Authentication Successful" ) == -1 ):
            trk.logfile( page + ".error", "url=%s\nstatus_code=%s\nRequest Headers:\n%s\n\nResponse Headers=\n%s\n\nResponse:\n%s" % ( url, response.status_code, response.request.headers, response.headers, response.text ) )
            raise Exception( "urika login failed: '%s'" % url )
        trk.stop( status_code = response.status_code )
        response.raise_for_status()

    def db_ensure_started( self ):
        if self.skip_admin() or self.options.dryhttp:
            return
        func = "db_ensure_started"
        trk = self.track.start( func )
        state = self.db_state()
        if ( state['status'] == 'STOPPED' ):
            self.db_start()
        elif ( state['status'] == 'STARTING' ):
            self.db_wait_while_starting( trk )
        elif ( state['status'] == 'COMPILING' ):
            self.db_wait_while_starting( trk )
        self.db_validate()
        trk.stop()

    def admin_get( self, path, verbose = False ):
        """HTTP GET to Urika admin API.
        """
        if self.skip_admin():
            return
        func = "admin_get"
        url = self.adminUrl + path
        trk = self.track.start( func, url = url )
        headers = { "Accept": "application/json" }
        if ( verbose ):
            self.log.debug("%s: %s", func, url )
        if (self.options.dryhttp):
            return
        response = self.session.get( url, headers=headers, timeout= 600 )
        if ( response.status_code == requests.codes.unauthorized ):
            self.log.warning( "%s %s, session may have timed out. Will login and try again." % ( func, response ) )
            self.login()
            response = self.session.get( url, headers=headers, timeout= 600 )
        if ( verbose ):
            self.log.debug( "%s: %s", func, response.text )
        trk.stop( status_code = response.status_code )
        response.raise_for_status()
        return response

    def admin_post( self, path, data ):
        """HTTP POST to Urika admin API.
        """
        if self.skip_admin():
            return
        func = "admin_post"
        url = self.adminUrl + path
        headers = { "Accept": "application/json" }
        trk = self.track.start( func, url = url )
        if self.options.dryhttp:
            return
        response = self.session.post( url, headers=headers, data=data, timeout= 1200 )
        if ( response.status_code == requests.codes.unauthorized ):
            self.log.warning( "%s %s, so session may have timed out. Will login and try again." % ( func, response ) )
            self.login()
            response = self.session.post( url, headers=headers, data=data, timeout= 1200 )
        self.log.debug( "%s: %s %s", func, response, response.text )
        trk.stop( status_code = response.status_code )
        response.raise_for_status()
        return response

    def admin_delete( self, path ):
        """HTTP DELETE to Urika admin API.
        """
        if self.skip_admin():
            return
        func = "admin_delete"
        url = self.adminUrl + path
        headers = { "Accept": "application/json" }
        trk = self.track.start( func, url = url )
        if self.options.dryhttp:
            return
        response = self.session.delete( url, headers=headers,
                                        timeout= self.options.query_timeout * 60 )
        if ( response.status_code == requests.codes.unauthorized ):
            self.log.warning( "%s %s, so session may have timed out. Will login and try again." % ( func, response ) )
            self.login()
            response = self.session.delete( url, headers=headers,
                                            timeout= self.options.query_timeout * 60 )
        trk.stop( status_code = response.status_code )
        response.raise_for_status()
        self.log.debug( "%s %s", func, response )
        return response

    def db_status( self ):
        if self.skip_admin():
            return
        try:
            response = self.admin_get( "db/current", verbose = False )
        except requests.exceptions.HTTPError, e:
            if str( e ).find( "404" ) > -1:
                return {}
            raise
        if self.options.dryhttp:
            return
        return response.json()

    def db_state( self ):
        """Abbreviated version of db_status.
        """
        if self.skip_admin():
            return
        status = self.db_status()
        if self.options.dryhttp:
            return
        updates = None
        if status.get( 'id' ):
            updates = self.admin_get( "db/%s/updates" % status[ 'id' ], verbose = False ).json()
        tmp = { 'name': status.get( 'name' ),
                'id': status.get( 'id' ),
                'status': status.get( 'databaseStatus' ),
                'running': status.get( 'running' ),
                'createdBy': status.get( 'createdBy' ),
                # Fa13 returns activeCheckpoint: None for a new database
                'activeCheckpointId': status[ 'activeCheckpoint' ][ 'id' ] if status.get( 'activeCheckpoint' ) is not None else None,
                # Fa13 alternative: self.checkpoint_active()[ 'id' ]
                'dirty': ( updates and
                           ( updates[ 'firstUpdateTime' ] or updates[ 'lastUpdateTime' ]
                             or updates[ 'insertsCount' ] > 0 or updates[ 'deletesCount' ] > 0
                             or len( updates[ 'insertedItems' ] ) > 0 ) ) }
        return tmp

    def db_dirty( self ):
        s = self.db_state()
        if s:
            return s[ 'dirty' ]

    def db_validate(self):
        """Validate name and status of DB.
        """
        if self.skip_admin():
            return {}
        old = self.state
        self.state = None
        new = self.db_state()
        if (self.options.dryhttp):
            return
        if ( old != new ):
            self.log.debug( "state: %s", new )
        if ( new[ 'status' ] == 'STOPPED' ):
            raise Exception("Database is stopped. Expected name '%s', got %s"
                            % ( self.options.db, new ) )
        if ( new[ 'name' ] and self.options.db != new[ 'name' ] ):
            raise Exception("Wrong database is running. Expected name '%s', got %s"
                            % ( self.options.db, new ) )
        if ( new['status'] == 'UPDATING' ):
            raise Exception( "Refusing to use db that is updating: %s" % new )
        if ( new[ 'status' ]
             and new['status'] != 'CONNECTED'
             and new['status'] != 'QUERYING' ):
            raise Exception( "Refusing to use db with state: %s" % new )
        self.state = new
        return new

    def build_query( self, label, blockTemplate, block= "", debug = True, **params ):
        text = blockTemplate.build( block, label= label, debug = debug, **params )
        if debug:
            paramsBrief = {}
            for k in params.keys():
                v = str( params[ k ] )
                if ( len( v ) > 8 ):
                    v = v[ 0:8 ]
                paramsBrief[ k ] = v
            return "# %s %s %s %s\n%s\n%s" % ( label, blockTemplate.name, block, paramsBrief, self.prologue, text )
        else:
            return "# %s %s %s\n%s\n%s" % ( label, blockTemplate.name, block, self.prologue, text )

    def query_get( self, label, text,
                   accept= "application/json",
                   timeout= None, retry= 0,
                   post= False, big_results= False, **extra ):
        """HTTP GET or POST to Sparql Query Endpoint.
        """
        # TODO: add headers for perfTracker:
        # urika.serv.PrintMemoryStatsRPC=1
        # urika.serv.PrintMemoryProcessSize=1
        # http://connect.us.cray.com/confluence/pages/viewpage.action?pageId=51184295&src=email
        func = "query_get" if ( not post ) else "query_post"
        url = self.queryUrl if ( not big_results ) else self.queryBigUrl
        headers = { "Accept": accept }
        params = { "query": text }
        if self.options.flavor == "fuseki" and accept.count( "json" ) > 0:
            # fuseki needs output param, not just accept header
            params[ 'output' ] = "json"
            # TODO: Urika accepts output=rj for construct queries (needed for Sp13), and see bug UR-1882
        # self.log.debug( "%s %s %s %s", func, label, url, headers )
        if self.options.dryhttp:
            return
        timeout_min = timeout
        if ( timeout_min is None ):
            timeout_min = self.options.query_timeout
        timeout_sec = timeout_min * 60 # convert minutes to seconds
        trk = self.track.start( func, url = url, label = label )
        response = None
        try:
            if ( post is True ):
                response = self.session.post( url, headers= headers, data= params, timeout= timeout_sec )
            else:
                response = self.session.get( url, headers= headers, params= params, timeout= timeout_sec )
        except requests.exceptions.SSLError, e:
            if ( retry > 0 ):
                self.log.warning( "%s Timeout (%s), retry %s %s, %s : %s", func, timeout_sec, url,
                               label, trk.sec(), e)
                self.log.debug( "%s: %s", func, traceback.format_exc( None ) )
                time.sleep( 5 )
                return self.query_get( label, text, accept, timeout_min, retry - 1, post, big_results )
            raise
        if ( response.status_code == requests.codes.unauthorized ):
            self.log.warning( "%s %s, so session may have timed out. Will login and try again.", func, response )
            self.login()
            return self.query_get( label, text, accept, timeout_min, retry - 1, post, big_results )

        trk.stop( status_code = response.status_code )
        # self.log.debug( "%s %s", func, response )
        return response

    def update_post( self, text, **dict ):
        """HTTP POST to Sparql Update Endpoint.
        """
        func = "update_post"
        url = self.updateUrl
        data = { "update": text }
        if (self.options.dryhttp):
            return
        trk = self.track.start( func, url = url )
        response = self.session.post( url, data=data, timeout= dict.get( 'timeout', self.options.update_timeout ) * 60 )
        if ( response.status_code == requests.codes.unauthorized ):
            self.log.warning( "%s %s, so session may have timed out. Will login and try again." % ( func, response ) )
            self.login()
            response = self.session.post( url, data=data, timeout= dict.get( 'timeout', self.options.update_timeout ) * 60 )

        trk.stop( status_code = response.status_code )
        # self.log.debug( "%s %s", func, response )
        return response

    def query( self, label, string = None, file = None, block= None, accept= "json", verbose= True, **params): #added oftype by TFY, ORNL to be able to query with strings and not just .rq files
        """Run Sparql query, return results.
        """
        func = "query"
        acceptTypes = { "csv": "text/csv",
                        "json": "application/sparql-results+json, application/json, text/javascript",
                        "xml": "application/sparql-results+xml" }

        if file:                                                                                    #
            basename = file.name[ : file.name.index( '.' ) ]                                        #
            if ( block is not None ):                                                               #
                basename = basename + "_" + block                                                   #
            if ( label is not None ):                                                               #   MODIFIED TFY, ORNL 7/22/2014
                label = label + "_" + basename                                                      #
            else:                                                                                   #
                label = basename                                                                    #
            trk = self.trackq.start( basename, label = label )                                      #
            text = self.build_query( label, file, block, **params )                                 #
                                                                                                    #
        elif string:                                                                                #
            text = string     #instead of the file the string is passed                             #
            trk = self.trackq.start(str(datetime.today()), label = label)                           #

        if ( verbose ):
            trk.logfile( label + ".rq", text )
        returnFormat = accept

        response_text = None
        try:
            response = self.query_get( label, text, acceptTypes.get( accept ), **params )
            trk.stop()
            if (self.options.dryhttp):
                return
            response_text = response.text
            response.raise_for_status()
            if ( verbose ):
                trk.logfile( label + "." + returnFormat, response_text )
            if (returnFormat == "json"):
                return response.json()
            else:
                return response_text
        except Exception, e:
            trk.error( response_text, exception = str( e ) )
            trk.logfile( label + ".error", response_text )
            raise

    def query_ask( self, label, file, block= None, **params ):
        """ASK query, return boolean.
        """
        result = self.query( label, file, block, **params )
        if ( self.options.dryhttp ):
            return False
        return result[ 'boolean' ]

    def query_single( self, label, file, block= None, **params ):
        """SELECT query, return first value.
        """
        result = self.query( label, file, block, **params )
        if ( self.options.dryhttp ):
            return 0
        first = result[ 'head' ][ 'vars' ][ 0 ]
        return result[ 'results' ][ 'bindings' ][ 0 ][ first ][ 'value' ]

    def query_int( self, label, file, block= None, **params ):
        """SELECT query, return first value as an int.
        """
        return int( self.query_single( label, file, block, **params ) )

    def query_first_binding( self, label, file, block= None, **params ):
        """SELECT query, return first binding (col) of each binding-set (row).
        """
        result = self.query( label, file, block, **params )
        if ( self.options.dryhttp ):
            return
        first = result[ 'head' ][ 'vars' ][ 0 ]
        return [ x[ first ][ 'value' ] for x in result[ 'results' ][ 'bindings' ] ]

    def count_graph( self, ngraph = None, verbose = False ):
        c = None
        if ( ngraph is None ):
            c = self.query_int( None, self.commonrq, "COUNT_DEFAULT", verbose = verbose, retry = 3 )
        else:
            c = self.query_int( None, self.commonrq, "COUNT_GRAPH", verbose = verbose, retry = 3, ngraph = ngraph )
        self.log.debug( "count_graph: %s %d", ngraph, c )
        return c

    def count_type( self, rdftype, ngraph = None, verbose = False ):
        c = None
        if ( ngraph is None ):
            c = self.query_int( None, self.commonrq, "COUNT_TYPE", verbose = verbose, rdftype = rdftype, retry = 1 )
        else:
            c = self.query_int( None, self.commonrq, "COUNT_TYPE_NGRAPH", verbose = verbose, rdftype = rdftype, retry = 1, ngraph = ngraph )
        return c

    def update( self, label, string = None, file = None, block= None, memfree = 0,**params ): #modified to accept strings also!
        """Run a SPARQL Update script.

        memfree is the ratio of serv memory expected to be needed to complete the query/update (example 0.25 is 500G on a 2T urika)
        """
        func = "update"
        if file:
            basename = file.name[ : file.name.index( '.' ) ]
            if ( block is not None ):
                basename = basename + "_" + block
            if ( label is not None ):
                label = label + "_" + basename
            else:
                label = basename
            self.db_freemem( self.dbmemmax * memfree )
            trk = self.tracku.start( basename, label = label )
            text = self.build_query( label, file, block, **params )
            trk.logfile( label + ".ru", text )
            # not needed in Fa13: self.db_validate()
        elif string:
            text = string    #instead of the file the string is passed
            trk = self.tracku.start(str(datetime.today()), label = label)
            trk.logfile( label + ".ru", text )

        response = self.update_post( text, **params )
        if (self.options.dryhttp):
            return
        if ( response.status_code == requests.codes.ok
             and response.text.find( "SPARQL Update ==> SUCCESS" ) ):
            trk.stop()
            extension = re.match( r"[a-zA-Z_]+/([a-zA-Z_]+).*",
                                  response.headers.get('content-type')
                                  ).group(1)
            trk.logfile( label + "." + extension, response.text )
        else:
            trk.error( response.text, response = str( response ) )
            trk.logfile( label + ".error", response.text )
            response.raise_for_status()

    def insert_data( self, label, data_text ):
        return self.update( label, self.commonrq, "INSERT_DATA",
                            data = data_text )

    def insert_rdfs_namespace( self ):
        """Load rdfs-namespace.xml, which was downloaded from http://www.w3.org/TR/rdf-schema/rdfs-namespace
        """
        import rdflib
        func = "insert_rdfs_namespace"
        graph = rdflib.Graph()
        graph.parse( relfile( __file__, "rdfs-namespace.xml" ), format="xml" )
        return self.insert_data( func, graph.serialize( format = 'nt' ) )

    def profile( self, ngraph, memfree = 0 ):
        """Inserts DB profile data into ngraph (drops first if it exists).
        """
        func = "profile"
        trk = self.track.start( func, ngraph = ngraph )
        solarpy.profile.profile.profile( self, ngraph, memfree = memfree )
        trk.stop()

    def update_cp( self, call_func, label, filename, block, timeout= 0, msg= "", ngraph = None, **params ):
        """DEPRECATED: use update and checkpoint_create individually.

        Run Update, then checkpoint.

        Also tracks time and change to triple count.
        """
        func = "update_cp"
        trk = self.trackc.start( call_func, label = label, filename = filename, block = block, ngraph = ngraph )
        before = None
        cp = None
        try:
            before = self.count_graph( ngraph )
            self.update( label, filename, block, timeout = max( timeout, self.options.update_timeout ), ngraph = ngraph, **params )
            cp = self.checkpoint_roll( label, msg )
        except Exception, e:
            trk.error( str( e ), triples_before = before, comment = msg )
            self.log.error( "%s %s %s: %s (triples before=%s cp=%s) %s: %s", func, call_func, trk.sec(), label,
                            before, cp, msg, e )
            raise
        diff = ( cp[ 'count_graph' ] - before )
        trk.stop( triples_before = before, triples_diff = diff, triples_dup = cp.get( 'count_dup' ), triples_graph = cp.get( 'count_graph' ) )
        cp[ 'count_before' ] = before
        cp[ 'count_diff' ] = diff
        return cp

    def checkpoint_active( self ):
        """Return json about the active checkpoint.
        """
        if self.skip_admin():
            return
        response = self.admin_get( "db/current/checkpoints/active", False )
        if self.options.dryhttp:
            return
        return response.json()

    def checkpoint_list( self, printout = False ):
        func = "checkpoint_list"
        trk = self.track.start( func )
        state = self.db_validate()

        # active_cpid = state.get( 'activeCheckpointId' )
        response = self.admin_get( "db/%s/checkpoint" % state[ 'id' ])
        items = response.json()[ "checkpoints" ]
        if ( printout ):
            # if len( items ) > 0:
            #     self.log.debug( "KEYS: %s" % items[ 0 ].keys() )
            #     self.log.debug( "CHECKPOINT %s", items[ 0 ] )
            headers = [ "id", "creationTime", "deletesCount", "insertsCount", "name", "description" ]
            s = StringIO()
            for h in headers:
                s.write( h )
                s.write( "\t" )
            s.write( "\n" )
            for item in items:
                for h in headers:
                    s.write( str( item.get( h ) ) )
                    s.write( "\t" )
                s.write( "\n" )
            print s.getvalue()
        trk.stop( item_ct = len( items ) )
        return items

    def checkpoint_create( self, label, comment = None, name = None, ngraph = None, verbose = True ):
        """Create a new checkpoint.
        """
        func = "checkpoint_create"
        if not self.db_dirty():
            return { 'count_before': None,
                     'count_graph': None }
        now = datetime.now().strftime( "%Y%m%dT%H%M" )
        if name is None:
            name = label + "_" + now
        trk = self.track.start( func, name = name )
        state = self.db_validate()

        comment = "" if comment is None else comment + " "
        comment += "(cmd=%s) (date=%s)" % ( pyname(), now )

        old_cpid = state.get( 'activeCheckpointId' )
        before = self.count_graph( ngraph ) if verbose else None
        if verbose:
            comment += " (count graph=%s before=%s)" % ( ngraph, before )

        if self.skip_admin():
            return { 'count_before': before,
                     'count_graph': before }
        # UR-2606 wait param set in non-standard way
        response = self.admin_post( "db/%s/checkpoint/create?wait=true" % state[ 'id' ],
                                    { 'name': name, 'comment': comment })
        if self.options.dryhttp:
            return {}
        result = response.json()
        cpid = result[ 'id' ]
        error = result[ 'error' ]
        if (error == None):
            self.log.debug("%s id=%s state=%s status=%s", func, cpid, result[ 'state' ], result.get( 'status' ) )
        else:
            raise Exception( "%s id=%s state=%s error=%s" % ( func, cpid, result[ 'state' ], error ) )

        # TODO: still needed? self.db_validate_by_query()

        after = self.count_graph( ngraph ) if verbose else None
        dup = ( before - after ) if verbose else None
        trk.stop( checkpoint_id = cpid, name = name, state = str( state ), graph_ct = after, duplicate_ct = dup )
        return { 'name': name,
                 'cpid': cpid,
                 'count_before': before,
                 'count_graph': after,
                 'count_dup': dup }

    def checkpoint_id_for_name( self, name ):
        """Find checkpoint id by name.
        """
        if self.skip_admin():
            return
        state = self.db_state()
        if (self.options.dryhttp):
            return
        response = self.admin_get( "db/%s/checkpoint" % state[ 'id' ] )
        for cp in response.json()[ 'checkpoints' ]:
            if ( cp[ 'name' ] == name ):
                return cp[ 'id' ]

    def checkpoint_restore( self, cpid, name=None ):
        """Restore a checkpoint by name.
        """
        if self.skip_admin():
            return
        func = "checkpoint_restore"
        trk = self.tracka.start( func, name = name, checkpoint_id = cpid )
        if (self.options.dryhttp):
            return

        if ( cpid == None and name != None):
            cpid = self.checkpoint_id_for_name( name )
        elif ( cpid != None and name != None):
            tmp = self.checkpoint_id_for_name( name )
            if ( tmp != cpid ):
                raise Exception( "%s given name '%s' and cpid '%s' do not match actual id for that name '%s'" % ( func, name, cpid, tmp  ) )

        state = self.db_validate()
        response = self.admin_post( "db/%s/checkpoint/%s/restore?wait=true" % ( state[ 'id' ], cpid ), {} )
        state = self.db_state()

        # TODO: still needed? BUG UR-2651 workaround, wait more even after status says okay
        # time.sleep( 5 )
        # self.login()
        # time.sleep( 5 )

        trk.stop( name = name, checkpoint_id = cpid, state = str( state ) )
        return cpid

    def checkpoint_delete( self, cpid ):
        """Delete a Urika checkpoint.
        """
        if self.skip_admin():
            return
        func = "checkpoint_delete"
        trk = self.tracka.start( func, checkpoint_id = cpid )
        self.log.debug("%s %s" % ( func, cpid ) )
        if (self.options.dryhttp):
            return
        state = self.db_validate()
        # DEL checkpoint a blocking call
        response = self.admin_delete( "db/%s/checkpoint/%s" % ( state[ 'id' ], cpid ) )
        state = self.db_validate()
        trk.stop( state = str( state ) )
        return cpid

    def checkpoint_delete_first_n( self, n ):
        """Delete first n Urika checkpoints.
        """
        if self.skip_admin():
            return
        func = "checkpoint_delete_first_n"
        if (self.options.dryhttp):
            return
        for i, item in enumerate( self.checkpoint_list() ):
            if i < n:
                self.checkpoint_delete( item[ "id" ] )

    def checkpoint_roll( self, label, comment, ngraph = None ):
        """Create a Urika checkpoint, ensure a unique name (timestamp).

        TODO: Then deletes the previously created checkpoint to save disk space.
        Does not delete any checkpoints not made by this program.
        """
        old_cp = self.checkpoint_created
        cp = self.checkpoint_create( label, comment, ngraph = ngraph )
        self.checkpoint_created = cp.get( 'cpid' )
        # TODO fix checkpoint_delete
        # if ( old_cp ):
        #     urika.checkpoint_delete( old_cp )
        return cp

    def db_wait_while_starting( self, trk ):
        """Poll db status until it's ready
        """
        if self.skip_admin():
            return
        func = "db_wait_while_starting"
        state = None
        while (True):
            state = None
            time.sleep( 5 )
            try:
                state = self.db_state()
                if ( state[ 'status' ] != 'STARTING'
                     and state[ 'status' ] != 'COMPILING' ):
                    # TODO BUG UR-2651 workaround, wait more even after status says okay
                    time.sleep( 5 )
                    return state
            except requests.exceptions.ConnectionError:
                self.log.debug( "%s: %s", func, traceback.format_exc() )
                time.sleep( 5 )
                self.login()
            seconds = trk.elapsed()
            if ( seconds > 900 ):
                raise Exception( "database still starting after t=%ds: %s" % ( seconds, state ) )

    def validate_by_query( self, label, file, block= None, **params ):
        """
        """
        func = "validate_by_query"
        trk = self.track.start( func, label = label )
        for i in range( 0, 10 ):
            try:
                self.query( label, file, block, **params )
                trk.stop()
                return # success
            except requests.exceptions.ConnectionError, e:
                self.log.warning( "%s %s: %s", func, trk.sec(), e )
                self.log.debug( "%s: %s", func, traceback.format_exc() )
                time.sleep( 10 )
                self.login()
            except requests.exceptions.SSLError, e:
                self.log.warning( "%s %s: %s", func, trk.sec(), e )
                self.log.debug( "%s: %s", func, traceback.format_exc() )
                time.sleep( 10 )
                self.login()
        self.query( label, file, block, **params )
        trk.stop()

    def db_validate_by_query( self ):
        s = self.db_validate()
        self.validate_by_query( None, self.commonrq, "ASK_NOTHING", verbose= False, retry= 0 )
        return s

    def db_list( self, printout = False ):
        func = "db_list"
        trk = self.track.start( func )
        response = self.admin_get( "db" )
        items = response.json()[ "database" ]
        if ( printout ):
            headers = [ "name", "id", "createdBy", "creationTime", "description", "databaseStatus", "updateState", "running", "allowedActions" ]
            s = StringIO()
            for h in headers:
                s.write( h )
                s.write( "\t" )
            s.write( "\n" )
            for item in items:
                # self.log.debug( "%s", item )
                for h in headers:
                    s.write( item.get( h ) )
                    s.write( "\t" )
                s.write( "\n" )
            print s.getvalue()
        trk.stop( item_ct = len( items ) )
        return items

    def db_for_name( self, name ):
        for item in self.db_list():
            if ( item[ "name" ] == name ):
                return item

    def db_start( self ):
        """Start the Urika database which is named in 'options.db'.

        If a different db is running, this will raise an exception.
        """
        if self.skip_admin():
            return
        func = "db_start"
        trk = self.tracka.start( func, db = self.options.db )
        mydb = self.db_for_name( self.options.db )
        state = self.db_state()
        if (self.options.dryhttp):
            return
        if ( state[ 'name' ] != self.options.db
             and state[ 'status' ] != "STOPPED"
             ):
            raise Exception( "Wrong database is running, '%s' with status '%s'. Expected name '%s', "
                             % ( state[ 'name' ], state[ 'status' ], self.options.db ) )
        if ( state[ 'status' ] == "CONNECTED" ):
            return
        if ( state[ 'status' ] == "STARTING" ):
            return
        # UR-2606: wait=true must be in the url, not a data parameter
        response = self.admin_post( "db/%s/start?wait=true" % mydb[ 'id' ], {} )
        self.log.debug("%s response=%s" % ( func, response.text ))
        state = self.db_validate()
        trk.stop( state = str( state ) )

    def db_restart( self ):
        """Call Urika Admin REST API to restart the DB (serv).
        """
        if self.skip_admin():
            return
        func = "db_restart"
        trk = self.tracka.start( func, db = self.options.db )
        state = self.db_validate()
        if (self.options.dryhttp):
            return

        state = self.db_state()
        if state and state[ 'dirty' ]:
            mem = self.db_mem()
            msg = "urika.py %s: DB is dirty, so checkpoint before restart. (mem=%sG free=%sG)" % ( func, mem.used, mem.free )
            self.log.warning( msg )
            self.checkpoint_create( "before_restart", msg )

        response = self.admin_post( "db/%s/restart?wait=true" % state[ 'id' ], {} )
        self.log.debug( "%s response=%s", func, response.text )
        used = self.dbmem_used()
        self.db_validate_by_query()
        trk.stop( dbmem_used = used )

    def db_stop( self ):
        """Call Urika Admin REST API to stop the DB (serv).
        """
        if self.skip_admin():
            return
        func = "db_stop"
        trk = self.tracka.start( func, db = self.options.db )
        state = self.db_validate()
        if self.options.dryhttp:
            return

        state = self.db_state()
        if state and state[ 'dirty' ]:
            mem = self.db_mem()
            msg = "urika.py %s: DB is dirty, so checkpoint before stop. (mem=%sG free=%sG)" % ( func, mem.used, mem.free )
            self.log.warning( msg )
            self.checkpoint_create( "before_stop", msg )

        response = self.admin_post( "db/%s/stop?wait=true" % state[ 'id' ], {} )
        trk.stop( db = self.options.db, dbid = state[ 'id' ], response = response.text )

    def data_import( self, name, filename, comment = '', format = "NT", copy = True ):
        func = "data_import"
        trk = self.track.start( func, name = name, comment = comment, format = format, copy = copy, filename = filename )
        response = self.admin_post( "import/local/form?wait=true",
                                    { 'name': name,
                                      'comment': comment,
                                      'filename': filename,
                                      'format': format,
                                      'copy': copy
                                      } )
        if (self.options.dryhttp):
            return
        # time.sleep( 15 )
        # state = self.db_wait_while_starting( trk )
        # state = self.db_validate()
        trk.stop( response = response.text )

    def data_items( self, printout = False ):
        func = "data_items"
        trk = self.track.start( func )
        response = self.admin_get( "import" )
        items = response.json()[ "item" ]
        if ( printout ):
            headers = [ "name", "id", "createdBy", "date", "description" ]
            s = StringIO()
            for h in headers:
                s.write( h )
                s.write( "\t" )
            s.write( "\n" )
            for item in items:
                for h in headers:
                    s.write( item.get( h ) )
                    s.write( "\t" )
                s.write( "\n" )
            print s.getvalue()
        trk.stop( item_ct = len( items ) )
        return items

    def data_item_for_name( self, name ):
        for item in self.data_items():
            if ( item[ "name" ] == name ):
                return item

    def db_create( self, comment, data_item_name ):
        func = "db_create"
        if ( self.options.db is None ):
            raise Exception( "options.db is: '%s'" % self.options.db )
        item = self.data_item_for_name( data_item_name )
        if ( item is None ):
            raise Exception( "data item not found with name: '%s'" % data_item_name )
        trk = self.tracka.start( func, name = self.options.db, item = item[ "id" ] )
        response = self.admin_post( "db?wait=true",
                                    { 'name': self.options.db,
                                      'comment': comment,
                                      'item': item[ "id" ]
                                      } )
        self.log.debug( "%s: %s", func, response.text )
        if self.options.dryhttp:
            return
        # time.sleep( 15 )
        # state = self.db_wait_while_starting( trk )
        state = self.db_validate()
        trk.stop( state = str( state ) )

    def switchDatabase(self, otherdb):
        func = "startAnotherDB"
        if self.options.db == otherdb: return
        self.db_stop()
        self.options.db = otherdb
        self.db_start()                                                                                                          #
        self.queryUrl = "%sdataset/sparql/%s/query" % ( self.options.url, self.options.db )                                 #  ADDED TFY, ORNL 7/22/2014
        self.queryBigUrl = "%sdataset/sparql/%s/all" % ( self.options.url, self.options.db )                                #  Adds abililty to change to another db in runtime.
        self.updateUrl = "%sdataset/sparql/%s/update" % ( self.options.url, self.options.db )                               #


    # def db_ensure_created( self, comment, data_item_name, data_item_filename, format = "NT", copy = True ):
    #     msg = datetime.now().strftime("%Y%m%dT%H%M") + " " + message
    #     item = self.data_item_for_name( "ivs_initial" )
    #     if ( item is None ):
    #         urika.data_import( "ivs_initial", filename, msg )
    #     urika.db_create( msg, "ivs_initial" )

    # # TODO
    # def check_system_resources(self):
    #     return

    def logfiles( self, printout = False ):
        if self.skip_admin():
            return
        func = "logfiles"
        trk = self.track.start( func )
        response = self.admin_get( "log" )
        items = response.json()[ "logResource" ]
        important = []
        for item in items:
            filename = item[ "resourceName" ]
            if ( filename.find( "sprinql_catalina.out" ) >= 0 ):
                important.append( str( filename ) )
            # elif ( filename.find( "gam_catalina.out" ) >= 0 ):
            #     important.append( filename )
        if ( printout ):
            headers = [ "category", "description", "resourceName", "lastModified", "size" ]
            s = StringIO()
            for h in headers:
                s.write( h )
                s.write( "\t" )
            s.write( "\n" )
            for item in sorted( items, key = lambda x: x[ "lastModified" ] ):
                # self.log.debug( "%s", item )
                for h in headers:
                    s.write( item.get( h ) )
                    s.write( "\t" )
                s.write( "\n" )
            print s.getvalue()
            print "important:", important
        trk.stop( item_ct = len( items ) )
        return important

    def dbmem_used( self ):
        stats = serv_stats()
        if ( stats == None ):
            return 0
        mem = stats[ 1 ]
        if ( mem == None ):
            return 0
        elif ( mem[ -1 ] == "G" ):
            return int( math.ceil( float( mem[ :-1 ] ) ) )
        elif ( mem[ -1 ] == "T" ):
            return int( math.ceil( float( mem[ :-1 ] ) * 1024 ) )
        elif ( mem[ -1 ] == "M" ):
            return int( math.ceil( float( mem[ :-1 ] ) / 1024 ) )

    def db_mem( self ):
        return DbMem( self.dbmemmax, self.dbmem_used() )

    def db_freemem( self, minfree ):
        """Restart the db if free mem of Urika GAD is less than minfree (GB, so 2000 = 2T).
        """
        func = "db_freemem"
        if minfree < 1:
            return
        mem = self.db_mem()
        if self.skip_admin():
            return mem
        if ( mem.free < minfree ):
            trk = self.track.start( func, dbmem_before = mem.used, dbmemfree_before = mem.free )
            self.log.info( "%s serv restarting mem used=%sG free=%sG", func, mem.used, mem.free )
            self.db_restart()
            mem = self.db_mem()
            trk.stop( dbmem = mem.used, dbmemfree = mem.free )
        return mem

def serv_stats():
    p = subprocess.Popen( [ 'mtatop -b' ], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    try:
        for line in p.stdout.readlines():
            if ( line.find( "serv" ) > -1 ):
                m = line.split()
                return ( m[ 10 ], m[ 6 ], m[ 8 ], m[ 7 ] )
    finally:
        p.terminate()

def process_stats( names ):
    p = subprocess.Popen( [ 'ps -a -o vsz,pmem,cpu,pcpu,time,command' ], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    try:
        res = []
        for line in p.stdout.readlines():
            for name in names:
                if ( line.find( name ) > -1 ):
                    m = line.split()
                    res.append( ( name, m[ 0 ], m[ 1 ], m[ 2 ], m[ 3 ],  m[ 4 ], m[ 5 ] ) )
        return res
    finally:
        p.terminate()

def monitor_log_proc( log, log_level, names ):
    for proc in process_stats( names ):
        name = proc[ 0 ]
        if ( len( proc ) == 0 ):
            log.log( log_level, "monitor: %s not found", name )
        else:
            try:
                pmem = float( proc[ 2 ] )
                pcpu = float( proc[ 4 ] )
                if ( ( pmem < 10.0 ) and ( pcpu < ( 20.0 ) ) ):
                    # log.log( log_level, "monitor: %s low0 %s %s %s", name, pmem, cpu, proc )
                    continue
                if ( ( pmem < 30.0 ) and ( pcpu < 5.0 ) ):
                    # log.log( log_level, "monitor: %s low1 %s %s %s", name, pmem, pcpu, proc )
                    continue
            except ValueError, e:
                print e
            proc = ( name, hbytes( proc[ 1 ] ), proc[ 2 ] + "%", proc[ 3 ], proc[ 4 ] + "%",  proc[ 5 ] )
            log.log( log_level, "monitor %s\t%s\t%s\t%s\t%s\t%s" % proc )

def monitor_logfile( log, filename, outdir = None):
    """
    """
    # TODO: monitor perfTracker
    # http://connect.us.cray.com/confluence/pages/viewpage.action?pageId=51184295&src=email
    watches = [ ( "[UPDT|DICT]lookup of .* was not found", None ), # GAD
                ( "[INFR]Warning! Unable to read rules file", None ), # GAD
                ( "[INFR]No rules provided for inferencing", None ), # GAD
                ( "[UPDT]writing", logging.DEBUG ), # insert and delete
                ( "[UPDT|REMD]numQuads", logging.DEBUG ), # insert
                ( "[UPDT]work count=", logging.DEBUG ), # delete
                ( "[QRY ]block 0 of 50 has", logging.INFO ),
                ( "Unable to load rules from file", None ), # GAD
                ( "The configured limit of 1,000 object references was reached while attempting to calculate the size of the object graph", logging.DEBUG ), # UR-2963, GAAS
                ( "[UPDT|SCRM]update received EAGAIN upon submission", None ), # GAD, temporary condition, not a bug, see bug806248
                ( "WARN", logging.WARN ),
                ( "ERROR", logging.ERROR ),
                ( "Remote killed by SIGTERM", logging.INFO ),
                ( "Remote killed by SIGKILL", logging.WARN ),
                ]
    p = subprocess.Popen( [ "tail -F -n -1 %s" % filename ], shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT )
    try:
        out = None
        if ( outdir is None ):
            log.info( "monitor_logfile: '%s'", filename )
        else:
            slash = filename.rfind( "/" )
            if ( slash >= 1 ):
                outfilename = outdir + filename[ slash : ]
                log.info( "monitor_logfile: '%s' '%s'", filename, outfilename )
                out = open( outfilename, "wb" )
            else:
                log.info( "monitor_logfile: '%s'", filename )
        try:
            while True:
                line = p.stdout.readline()
                if ( not line ):
                    break
                if ( out ):
                    out.write( line )
                    out.flush()
                for watch in watches:
                    # TODO: feature to print a watch message only once
                    if ( line.find( watch[ 0 ] ) > -1 ):
                        level = watch[ 1 ]
                        if ( level is not None ):
                            log.log( level, filename + ": " + line.encode( 'ascii', 'replace' ).strip() )
                        break
        finally:
            if ( out ):
                out.close()
    finally:
        p.terminate()

def monitor( options, log, log_level ):
    """
    """
    myname = sys.argv[ 0 ]
    a = myname.rfind( "/" )
    if ( a >= 0 ):
        myname = myname[ a + 1 : ]
    prev = None
    while True:
        try:
            serv = serv_stats()
            curr = None
            if ( serv ):
                curr = ( "%s %s %s %s" % ( serv[ 0 ],
                                           serv[ 1 ][ :-2 ],
                                           serv[ 2 ][ :-2 ],
                                           serv[ 3 ][ :-4 ] ) )
                # log.debug( "monitor serv %s prev= %s curr= %s", (prev != curr), prev, curr )
                if ( prev != curr):
                    log.log( log_level, "monitor %s\t%s\t\t\t%s\t%s" % serv )
            else:
                curr = "serv not found"
                if ( prev != curr):
                    log.log( log_level, "monitor: %s", curr )
            prev = curr

            monitor_log_proc( log, logging.DEBUG,
                              [ myname,
                                "fuseki",
                                "sprinql", "gam", "elasticsearch", "logstash", "postgres" ] )

            time.sleep( options.monitor )
        except Exception, e:
            log.warning( "monitor: %s", e )
            time.sleep( options.monitor )

#
# Main
#

def init_logging( options ):
    """Logging, see http://docs.python.org/2.6/library/multiprocessing.html#logging
    and see http://docs.python.org/2.6/library/logging.html#module-logging
    """
    log = logging.getLogger("Main")
    log.setLevel( logging.DEBUG )
    console = logging.StreamHandler()
    if options.debug:
        console.setLevel( logging.DEBUG )
    else:
        console.setLevel( logging.INFO )
    # set a format which is simpler for console use
    console.setFormatter( logging.Formatter( '[%(levelname)-1s %(timehms)s] %(message)s' ) )
    log.addHandler( console )
    log = logging.LoggerAdapter( log, LogExtra() )
    return log

class UrikaOptions ( object ):

    def __init__(self):
        self.debug = False
        self.admin = True
        self.url = None
        self.monitor = 10
        self.db = None
        self.username = None
        self.password = None
        self.query_timeout = 20
        self.update_timeout = 30
        self.baseURI = "http://urika/"
        self.dryrun = False
        self.dryhttp = False
        self.logdir = None
        self.resultsDir = None
        self.flavor = "urika"
        self.dbmemmax = 2000

def prepare_options( parser, defaults ):
    parser.add_option( "-v", "-d", "--debug", action="store_true", default = defaults.debug,
                       help="verbose debug logging to stderr [%default]")
    parser.add_option("--flavor", metavar="NAME", default= defaults.flavor,
                      help="DB flavor (urika, fuseki) [%default]")
    parser.add_option("--url", metavar="URL", default= defaults.url,
                      help="Urika URL, for example https://login1/ [%default]")
    parser.add_option("--db", metavar="NAME", default= defaults.db,
                      help="Urika database name [%default]")
    parser.add_option("--username", metavar="NAME", default= defaults.username,
                      help="Urika GAM username (must be Admin) [%default]")
    parser.add_option("--password", metavar="STRING",
                      help="Urika GAM password [%default]")
    parser.add_option("--query_timeout", default= defaults.query_timeout, type="int", metavar="MIN",
                      help="Default client-side timeout minutes for queries [%default]")
    parser.add_option("--update_timeout", default= defaults.update_timeout, type="int", metavar="MIN",
                      help="Default client-side timeout minutes for updates [%default]")
    parser.add_option( "--dbmemmax", metavar="NAME", default= defaults.dbmemmax,
                       help="Urika database memory maximum size in GB (2000 is 2T) [%default]")
    parser.add_option("--admin", "--admin", action="store_true", default = defaults.admin,
                      help="Use Urika admin API [true]")
    parser.add_option( "--dryrun", action="store_true", default = defaults.dryrun,
                       help="Show what will be done, but do not process files or run queries [%default]")
    parser.add_option("--dryhttp", action="store_true", default = defaults.dryhttp,
                      help="Like dryrun, process files, but do not execute any http calls or queries [%default]")
    parser.add_option("--baseURI", action="store_true", default = defaults.baseURI,
                      help=" [%default]")
    parser.add_option("--monitor", default= defaults.monitor, type="int", metavar="SEC",
                      help="Monitor urika (serv) during other processing [%default]")
    parser.add_option("--logdir", metavar="DIR", default= defaults.logdir,
                      help="write log files to DIR [%default]")
    parser.add_option("--resultsDir", metavar="DIR", default= defaults.resultsDir,
                      help="write output files to DIR [%default]")

def init_options():
    """Init from command line args.

    See http://docs.python.org/2/library/optparse.html
    optparse is deprecated in 2.7, but Urika service nodes have 2.6.8
    """
    parser = optparse.OptionParser( usage= """

  %prog [options]

Name:

  Urika command line API for query, update, monitoring, and admin functions.
  Copyright 2013-2104 YarcData LLC, a Cray Company. All Rights Reserved.""" )
    parser.add_option( "-r", "--run", metavar="OPERATION",
                       help="Operation to run [%default]" )

    prepare_options( parser, UrikaOptions() )

    # parse_args will automatically exit if --help is used
    ( options, args ) = parser.parse_args()

    if (options.dryrun == None):
        options.dryrun = False
    if (options.dryhttp == None):
        options.dryhttp = False
    if (options.dryrun):
        options.dryhttp = True # logical implication

    return ( options, args )

def main_run( options, args, log ):
    if ( options.run is not None ):
        tracking = Tracking( options, log )
        track = tracking.context( tracking.ns.urika, logging.DEBUG )
        urika = Urika( options, log, logging.DEBUG, track )
        urika.login()
        if ( options.run == "db_stop" ):
            urika.db_stop()
        elif ( options.run == "db_start" ):
            urika.db_start()
        elif ( options.run == "db_restart" ):
            urika.db_restart()
        elif ( options.run == "db_state" ):
            print urika.db_state()
        elif ( options.run == "db_status" ):
            print urika.db_status()
        elif ( options.run == "count_graph" ):
            print urika.count_graph()
        elif ( options.run == "query" ):
            print urika.query_get( "", args[ 0 ] ).text        # UID: TFY  CHANGED 4/14/2014 16:12
            #return (urika.query_get( "", args[ 0 ] ).text)
        elif ( options.run == "update" ):
            print urika.update_post( args[ 0 ] ).text
        elif ( options.run == "data_items" ):
            urika.data_items( printout = True )
        elif ( options.run == "data_import" ):
            urika.data_import( args[ 0 ], args[ 1 ], arg[ 2 ] )
        elif ( options.run == "db_list" ):
            urika.db_list( printout = True )
        elif ( options.run == "db_create" ):
            urika.db_create( args[ 0 ], args[ 1 ] )
        elif ( options.run == "logfiles" ):
            urika.logfiles( printout = True )
        elif ( options.run == "monitor_log" ):
            monitor_logfile( log, urika.logfiles()[ 0 ] )
        elif ( options.run == "checkpoint" ):
            print urika.checkpoint_create( args[ 0 ], verbose = options.debug )
        elif ( options.run == "checkpoint_list" ):
            urika.checkpoint_list( printout = True )
        elif ( options.run == "checkpoint_delete" ):
            urika.checkpoint_delete( args[ 0 ] )
        elif ( options.run == "checkpoint_delete_first_n" ):
            urika.checkpoint_delete_first_n( int( args[ 0 ] ) )
        elif ( options.run == "db_validate" ):
            print urika.db_validate_by_query()
        elif ( options.run == "profile" ):
            urika.profile( args[ 0 ] )

def main():
    ( options, args ) = init_options()
    log = init_logging( options )

    mon1 = None
    try:
        if ( options.run is not None ):
            if ( options.monitor > 0 ):
                mon1 = process( monitor, options, log, logging.INFO )
                mon1.start()
            main_run( options, args, log )
        else:
            monitor( options, log, logging.INFO )
    finally:
        if mon1:
            # time.sleep( options.monitor )
            mon1.terminate()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass

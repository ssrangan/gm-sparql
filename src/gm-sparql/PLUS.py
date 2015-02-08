#------------------ UManage.py ------------------#
#  connection to urika or local fuskei endpoint  #
#------- TFY (Tyler Brown) ------ 6/26/2014 -----#

class initoptions ():

    def __init__(self, flavor):

        if flavor == "urika":
            self.url = "https://apollo.ccs.ornl.gov/"
            self.flavor = "urika"
        elif flavor == "fuseki":
            self.url = "http://localhost:3030/"
            self.flavor = "fuseki"

        self.debug = False
        self.admin = True
        self.monitor = 10
        self.db = None
        self.username = None
        self.password = None
        self.query_timeout = 60
        self.update_timeout = 60
        self.baseURI = "http://urika/"
        self.dryrun = False
        self.dryhttp = False
        self.logdir = None
        self.resultsDir = None
        self.dbmemmax = 2000


class initsolarpy ():

    def __init__ (self, usern, passw, db, flavor):
        import logging
        import traceback
        import optparse
        import solarpy.yarc.urika as Urika
        from solarpy.yarc.Tracking import Tracking

        self.options = initoptions(flavor)
        self.options.db = db
        self.options.username = usern
        self.options.password = passw
        self.options.query_timeout = 60 * 24
        self.options.update_timeout = 60 * 24

        if flavor == "fuseki": self.options.url = "%s%s/" %(self.options.url, db)

        log = logging.getLogger("solarpyinit")
        tracking = Tracking( self.options, log )
        track = tracking.context( tracking.ns.urika, logging.DEBUG )
        self.urika = Urika.Urika( self.options, log, logging.DEBUG, track )
        self.urika.login()

class initwrapper():
    def __init__ (self, usern, passw, db, flavor):
        from SPARQLWrapper import SPARQLWrapper, JSON

        options = initoptions(flavor)
        options.db = db
        options.username = usern
        options.password = passw


        if flavor == "urika":
            self.sparql = SPARQLWrapper("%sbasic/ds/%s/query" %(options.url, options.db), "%sbasic/ds/%s/update" %(options.url, options.db))
            self.sparql.setCredentials(options.username, options.password)

        elif flavor == "fuseki":
            options.url = "%s%s" %(options.url, db) #no trailing foward slash
            self.sparql = SPARQLWrapper("%s/query" %options.url, "%s/update" %options.url)


        self.sparql.setReturnFormat(JSON)

def _urikaCheck():
    import platform
    import subprocess

    os = platform.system()

    if os.lower() == "windows":
        out = subprocess.check_output('netstat -np tcp | find "443" | find "ESTABLISHED"', shell = True)
        seek = out.find("172.30.246.4") #urika's IP address

    elif os.lower() == "linux":
        out = subprocess.check_output('netstat --tcp', shell = True)
        seek = out.find("apollo")
    else:
        print "WARNING: An attempt was made to determine if an exsisting Urika connection is present, it failed! - PROCEED WITH CAUTION"
        return(True)

    if seek == -1: # ip not found in netstat
        return(True)
    else:
        print "WARNING: An existing connection to Urika active. Trying to disable your RSAToken, are you?!"
        return(False)

def UrikaLogin(usern, passw, db = None, method = "solarpy"):
    if _urikaCheck():
        if method == "solarpy":
            solarpy = initsolarpy(usern, passw, db, flavor = "urika")
            return(solarpy)

        elif method == "sparqlwrapper":
            wrapper = initwrapper(usern, passw, db, flavor = "urika")
            return(wrapper)
    else:
        exit()

def FusekiLogin(db, method = "solarpy"):
    if method == "solarpy":
        solarpy = initsolarpy(None, None, db, flavor = "fuseki")
        return(solarpy)
    elif method == "sparqlwrapper":
        wrapper = initwrapper(None, None, db, flavor = "fuseki")
        return(wrapper)

if __name__ == '__main__':
    pass




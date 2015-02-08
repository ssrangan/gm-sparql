#-------------------------------------------------------------------------------
# 2.2 Added check for active login
#-------------------------------------------------------------------------------
from PLUS import UrikaLogin
from PLUS import FusekiLogin

def main():
    func = "mainlogin"
    method ="solarpy"

    #conn = FusekiLogin("ds")

    conn = UrikaLogin("tfy", raw_input("provide password"))
    if method == "solarpy":
        query = "SELECT * WHERE { ?s ?p ?o } LIMIT 10"
        results = conn.urika.query( func, query, accept = "csv", big_results = True)
        print results

    elif method == "sparqlwrapper":
        conn.sparql.setQuery("SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        conn.sparql.method = "GET"
        results = conn.sparql.query().convert()
        for result in results["results"]["bindings"]:
            print(result)

##    conn.urika.startAnotherDB("test1")
##    resp = conn.urika.db_status()
##    print resp['name']
##    print "----------------------------------------------------------------"
##    query = "SELECT * WHERE { ?s ?p ?o } LIMIT 10"
##    results = conn.urika.query( func, string = query, accept = "csv")
##    print results


    if method =="solarpy":
#QUERY FILE:
##        query = conn.urika.blockt( __file__, "test.rq")
##        results = conn.urika.query( func, file = query, accept = "csv")

##        query = "SELECT * WHERE { ?s ?p ?o } LIMIT 10"
##        results = conn.urika.query( func, query, None, "csv", oftype = "string")

##        print results
##        dbs = []
##        admindb = conn.urika.admin_get("db").json()['database']
##        for i in range(len(admindb)):
##            db = {
##            'id': admindb[i]['id'],
##            'name': admindb[i]['name'],
##            'href': admindb[i]['link'][1]['href']
##            }
##            dbs.append(db)
##        print dbs
        pass
#UPDATE STRING:


        update = """PREFIX dc: <http://purl.org/dc/elements/1.1/>
INSERT DATA
{ <http://example/book3> dc:title    "A new book" ;
                         dc:creator  "A.N.Other" .
}"""

        conn.urika.update(func, string  = update)




    elif method == "sparqlwrapper":

# UPDATE:
##        update = """PREFIX dc: <http://purl.org/dc/elements/1.1/>
##INSERT DATA
##{ <http://example/book3> dc:title    "A new book" ;
##                         dc:creator  "A.N.Other" .
##}"""
##        conn.sparql.setQuery(update)
##        conn.sparql.method = "POST"
##        post = conn.sparql.query()

# QUERY:
        conn.sparql.setQuery("SELECT * WHERE { ?s ?p ?o } LIMIT 10")
        conn.sparql.method = "GET"
        results = conn.sparql.query().convert()
        for result in results["results"]["bindings"]:
            print(result)

if __name__ == '__main__':
    main()

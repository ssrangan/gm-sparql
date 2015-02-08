 INSERT
        {
            GRAPH <%s>
            { ?s <%s> ?o }
        }
        WHERE
        {
            SELECT DISTINCT ?s ?o
            WHERE
            { ?s <%s> ?o . }
        } ;
		

        INSERT
        {
            GRAPH <%s>
            {
              ?s <%s/prevPR> %s
            }
        }
        WHERE
        {
            SELECT DISTINCT ?s
            WHERE {
				{ ?s <%s> ?o . }
				union
				{ ?o <%s> ?s . }
			}
        };

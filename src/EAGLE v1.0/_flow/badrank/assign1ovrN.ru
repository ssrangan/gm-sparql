        INSERT
        {
            GRAPH <%s>
            {
              ?s <%s/prevBR> %s
            }
        }
        WHERE
        {
            SELECT DISTINCT ?s
            WHERE {
				{ ?s <%s> ?o . }
				UNION
				{ ?o <%s> ?s . }
			}
        };

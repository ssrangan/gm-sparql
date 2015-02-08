DELETE { GRAPH <%s>
 { ?s <%s/newPR> ?o } 
} WHERE 
{ GRAPH <%s> {?s <%s/newPR> ?o } };

INSERT { GRAPH <%s> 
{ ?s <%s/newPR> ?Contribution . }}
WHERE {
SELECT ?s ((sum(?val2/?val1)*%s)+%s as ?Contribution)
WHERE {
    {
        {?x <%s> ?s.}
        { graph  <%s>
         {
            ?x  <%s/outDeg> ?val1 . 
            ?x  <%s/prevPR> ?val2 
         }
        }
    }
}
GROUP BY ?s};


INSERT { GRAPH <%s> 
{ ?s <%s/newPR> ?Contribution.}} 
WHERE
{
    SELECT ?s (?val2 * (1 - %s) + %s as ?Contribution)
    WHERE {
        {
            {?s <%s> ?o.}
            { graph  <%s>
             {
                ?s  <%s/prevPR> ?val2 
             }
            }
            filter not exists {?x <%s> ?s.}
        }
    }

};

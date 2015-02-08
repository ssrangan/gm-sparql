INSERT { GRAPH <%s> 
{ ?x <%s/outDeg> ?degree }}
WHERE {
SELECT ?x (count(*) as ?degree) 
{
    {?x ?p ?o}
UNION
    {?s ?p ?x}
}
GROUP BY ?x
 }
 

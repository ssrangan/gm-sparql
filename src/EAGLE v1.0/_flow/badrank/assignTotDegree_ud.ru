INSERT { GRAPH <%s> 
{ ?x <%s/Deg> ?degree }}
WHERE {
SELECT ?x (count(*) as ?degree) 
{
    {?x ?p ?o}
UNION
    {?s ?p ?x}
}
GROUP BY ?x
 }
 

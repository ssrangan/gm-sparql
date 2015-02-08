INSERT { GRAPH <%s> 
{ ?s <%s/outDeg> ?outdegree }}
WHERE {
SELECT ?s (COUNT(*) AS ?outdegree) 
{ ?s ?p ?o }
GROUP BY ?s
 }
 

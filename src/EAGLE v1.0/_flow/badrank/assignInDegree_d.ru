INSERT { GRAPH <%s> 
{ ?o <%s/Deg> ?indegree }}
WHERE {
SELECT ?o (COUNT(*) AS ?indegree) 
{ ?o ?p ?s }
GROUP BY ?o
 }
 

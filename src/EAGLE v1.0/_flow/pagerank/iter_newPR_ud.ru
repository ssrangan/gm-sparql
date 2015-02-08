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
{graph  <%s>{ 
 ?x  <%s/outDeg> ?val1 . 
 ?x  <%s/prevPR> ?val2 }}
}
union
{
{?s <%s> ?x.}
{graph  <%s>{ 
 ?x  <%s/outDeg> ?val1 . 
 ?x  <%s/prevPR> ?val2 }}
}
}
GROUP BY ?s}

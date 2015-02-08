DELETE { GRAPH <%s>
 { ?s <%s/prevPR> ?o } 
} WHERE 
{ GRAPH <%s> {?s <%s/prevPR> ?o . ?s <%s/newPR> ?o1} };

INSERT { GRAPH <%s> 
{ ?s <%s/prevPR> ?o . }}
WHERE {
SELECT ?s ?o
WHERE { graph  <%s>
{
?s <%s/newPR> ?o
}
}
}



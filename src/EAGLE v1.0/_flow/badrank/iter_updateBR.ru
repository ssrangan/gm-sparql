DELETE { GRAPH <%s>
 { ?s <%s/prevBR> ?o } 
} WHERE 
{ GRAPH <%s> {?s <%s/prevBR> ?o . ?s <%s/newBR> ?o1} };

INSERT { GRAPH <%s> 
{ ?s <%s/prevBR> ?o . }}
WHERE {
SELECT ?s ?o
WHERE { graph  <%s>
{
?s <%s/newBR> ?o
}
}
}



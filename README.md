# gm-sparql
Graph Mining Using SPARQL

The Resource Description Framework (RDF) and SPARQL Protocol and RDF Query Language (SPARQL) were introduced about a decade ago to enable flexible schema-free data interchange on the Semantic Web. Today, data scientists use the framework as a scalable graph representation for integrating, querying, exploring and analyzing data sets hosted at different sources. With increasing adoption, the need for graph mining capabilities for the Semantic Web has emerged. We address that need through implementation of popular iterative Graph Mining algorithms (degree distribution, diameter, radius, node eccentricity, triangle count, connected component analysis, and PageRank). We implement these algorithms as SPARQL queries, wrapped within Python scripts. These graph mining algorithms (that have a linear-algebra formulation) can indeed be unleashed on data represented as RDF graphs using the SPARQL query interface.

Although primarily developed for Cray's Urika hosted at the Department of Energy's Oak Ridge National Laboratory, this open source version works on Apache Jena triplestore. We have tested EAGLE to work on desktops, laptops and cloud services such as Amazon EC2. EAGLE can play a critical role in the exploratory analysis of massive heterogeneous graph data (e.g. semantic knowledge graphs). We believe with more support and user feedback we can create a "MATLAB" for LinkedData.

We really appreciate citing the following paper if our code was useful in anyway to your work.

S.Lee (lees4@ornl.gov) , S.R. Sukumar (sukumarsr@ornl.gov) and S- H. Lim (lims1@ornl.gov), ”Graph mining meets the Semantic Web”, in the Proc. of the Workshop on Data Engineering meets the Semantic Web in conjunction with International Conference on Data Engineering, Korea, 2015.

List of ORNL team members (in alphabetical order)
* Sangkeun Lee (lees4@ornl.gov, leesangkeun@gmail.com) - Post-doc
* Seung-Hwan. Lim (lims1@ornl.gov)
* Seokyong Hong (shong3@ncsu.edu) - Intern
* Sreenivas R. Sukumar (sukumarsr@ornl.gov)
* Tyler C. Brown (browntc@ornl.gov) - Intern


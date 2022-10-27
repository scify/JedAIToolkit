<p align="center"> 
<img src="https://github.com/scify/JedAIToolkit/blob/master/documentation/JedAI_logo_small.png">
</p>

**Please check [pyJedAI](https://github.com/AI-team-UoA/pyJedAI) for an implementation of JedAI in native Python.**

Please check our [paper](https://github.com/scify/JedAIToolkit/blob/master/documentation/JedAI_3D_ER.pdf) for a detailed description of version 3.0. 

The code for running JedAI on **Apache Spark** is available [here](https://github.com/scify/JedAI-Spark). 

The **Web Application** for running JedAI is available [here](https://github.com/GiorgosMandi/JedAI-WebApp). A video explaining how to use it is available [here](https://www.youtube.com/watch?v=OJY1DUrUAe8).

JedAI is also available as a **Docker image** [here](https://hub.docker.com/repository/docker/gmandi/jedai-webapp). See below for more details.

The latest version of JedAI-gui is available [here](jedai-ui.7z).

# Java gEneric DAta Integration (JedAI) Toolkit
JedAI constitutes an open source, high scalability toolkit that offers out-of-the-box solutions for any data integration task, e.g., Record Linkage, Entity Resolution and Link Discovery. At its core lies a set of *domain-independent*, *state-of-the-art* techniques that apply to both RDF and relational data. These techniques rely on an approximate, *schema-agnostic* functionality based on *(meta-)blocking* for high scalability. 

JedAI can be used in three different ways:

  1) As an **open source library** that implements numerous state-of-the-art methods for all steps of the end-to-end ER work presented in the figure below.
  2) As a [**desktop application**](https://github.com/scify/jedai-ui) with an intuitive Graphical User Interface that can be used by both expert and lay users.
  3) As a **workbench** that compares the relative performance of different (configurations of) ER workflows.
  
This repository contains the code (in Java 8) of JedAI's open source library. The code of JedAI's desktop application and workbench is available in this [repository](https://github.com/scify/jedai-ui). 

Several **datasets** already converted into the serialized data type of JedAI can be found [here](./data).

You can find a short presentation of JedAI Toolkit [here](documentation/JedAIpresentation.pptx).

### Citation

If you use JedAI, please cite the following paper:

*George Papadakis, Leonidas Tsekouras, Emmanouil Thanos, George Giannakopoulos, Themis Palpanas and Manolis Koubarakis: "The return of JedAI: End-to-End Entity Resolution for Structured and Semi-Structured Data", in VLDB 2018* ([pdf](http://www.vldb.org/pvldb/vol11/p1950-papadakis.pdf)).

### Consortium

JEDAI is a collaboration project involving the following partners:
* [Department of Informatics and Telecommunications, University of Athens](http://www.di.uoa.gr),
* [Software and Knowledge Engineering Lab, National Center for Scientific Research "Demokritos"](https://www.iit.demokritos.gr/labs/skel/) ,
* [Science-For-You not-for-profit company](https://www.scify.gr/site/en) 
* [LIPADE, Paris Descartes University](http://lipade.mi.parisdescartes.fr)

## JedAI Workflow

JedAI supports 3 workflows, as shown in the following images:

<img src="documentation/workflow1.png" height="80">
<img src="documentation/workflow2.png" height="80">
<img src="documentation/workflow3.png" height="80">

Below, we explain in more detail the purpose and the functionality of every step.

### Data Reading 
It transforms the input data into a list of entity profiles. An entity is a uniquely identified set of name-value pairs (e.g., an RDF resource with its URI as identifier and its set of predicates and objects as name-value pairs). 

The following formats are currently supported:
 1) CSV 
 2) RDF in any format, i.e., XML, OWL, HDT, JSON
 3) Relational Databases (mySQL, PostgreSQL)
 4) SPARQL endpoints
 5) Java serialized objects
 
### Schema Clustering

This is an optional step, suitable for highly heterogeneous datasets with a schema comprising a large diversity of attribute names. To this end, it groups together attributes that are syntactically similar, but are not necessarily semantically equivalent. 

The following methods are currently supported:
1) Attribute Name Clustering
2) Attribute Value Clustering
3) Holistic Attribute Clustering

For more details on the functionality of these methods, see [here](http://www.vldb.org/pvldb/vol9/p312-papadakis.pdf).  
  
### Block Building 
It clusters entities into overlapping blocks in a lazy manner that relies on unsupervised blocking keys: every token in an attribute value forms a key. Blocks are then extracted, possibly using a transformation, based on its equality or on its similarity with other keys.

The following methods are currently supported:
 1) Standard/Token Blocking
 2) Sorted Neighborhood
 3) Extended Sorted Neighborhood
 4) Q-Grams Blocking
 5) Extended Q-Grams Blocking
 6) Suffix Arrays Blocking
 7) Extended Suffix Arrays Blocking
 8) LSH MinHash Blocking
 9) LSH SuperBit Blocking
  
For more details on the functionality of these methods, see [here](https://github.com/scify/JedAIToolkit/blob/master/documentation/JedAI_3D_ER.pdf).  

### Block Cleaning
Its goal is to clean a set of overlapping blocks from unnecessary comparisons, which can be either *redundant* (i.e., repeated comparisons that have already been executed in a previously examined block) or *superfluous* (i.e., comparisons that involve non-matching entities). Its methods operate on the coarse level of individual blocks or entities.

The following methods are currently supported:
 1) Size-based Block Purging
 2) Cardinality-based Block Purging
 3) Block Filtering
 4) Block Clustering
 
All methods are optional, but complementary with each other and can be used in combination. For more details on the functionality of these methods, see [here](http://www.vldb.org/pvldb/vol9/p684-papadakis.pdf).  

### Comparison Cleaning
Similar to Block Cleaning, this step aims to clean a set of blocks from both redundant and superfluous comparisons. Unlike Block Cleaning, its methods operate on the finer granularity of individual comparisons. 

The following methods are currently supported:
 1) Comparison Propagation
 2) Cardinality Edge Pruning (CEP)
 3) Cardinality Node Pruning (CNP)
 4) Weighed Edge Pruning (WEP)
 5) Weighed Node Pruning (WNP)
 6) Reciprocal Cardinality Node Pruning (ReCNP)
 7) Reciprocal Weighed Node Pruning (ReWNP)
 8) BLAST
 9) Canopy Clusetring
 10) Extended Canopy Clustering

Most of these methods are Meta-blocking techniques. All methods are optional, but competive, in the sense that only one of them can part of an ER workflow. For more details on the functionality of these methods, see [here](http://www.sciencedirect.com/science/article/pii/S2214579616300168). They can be combined with one of the following weighting schemes:
   1) Aggregate Reciprocal Comparisons Scheme (ARCS)
   2) Common Blocks Scheme (CBS)
   3) Enhanced  Common  Blocks  Scheme (ECBS)
   4) Jaccard Scheme (JS)
   5) Enhanced  Jaccard  Scheme (EJS)
   6) Pearson chi-squared test

### Entity Matching
It compares pairs of entity profiles, associating every pair with a similarity in [0,1]. Its output comprises the *similarity graph*, i.e., an undirected, weighted graph where the nodes correspond to entities and the edges connect pairs of compared entities. 

The following schema-agnostic methods are currently supported:
  1) [Group Linkage](http://pike.psu.edu/publications/icde07.pdf), 
  2) Profile Matcher, which aggregates all attributes values in an individual entity into a textual representation.

Both methods can be combined with the following representation models.
  1) character n-grams (n=2, 3 or 4)
  2) character n-gram graphs (n=2, 3 or 4)
  3) token n-grams (n=1, 2 or 3)
  4) token n-gram graphs (n=1, 2 or 3)

For more details on the functionality of these bag and graph models, see [here](https://link.springer.com/article/10.1007%2Fs11280-015-0365-x).

The bag models can be combined with the following similarity measures, using both TF and TF-IDF weights: 
   1) ARCS similarity
   2) Cosine similarity 
   3) Jaccard similarity 
   4) Generalized Jaccard similarity 
   5) Enhanced Jaccard similarity
   
The graph models can be combined with the following graph similarity measures:
   1) Containment similarity 
   2) Normalized Value similarity 
   3) Value similarity 
   4) Overall Graph similarity
   
Any word or character-level pre-trained embeddings are also supported in combination with cosine similarity or Euclidean distance.

### Entity Clustering
It takes as input the similarity graph produced by Entity Matching and partitions it into a set of equivalence clusters, with every cluster corresponding to a distinct real-world object.

The following domain-independent methods are currently supported for Dirty ER:
  1) Connected Components Clustering
  2) Center Clustering
  3) Merge-Center Clustering
  4) Ricochet SR Clustering
  5) Correlation Clustering
  6) Markov Clustering
  7) Cut Clustering

For more details on the functionality of these methods, see [here](http://dblab.cs.toronto.edu/~fchiang/docs/vldb09.pdf). 

For Clean-Clean ER, the following methods are supported:
  1) Unique Mapping Clustering
  2) Row-Column Clustering
  3) Best Assignment Clustering

For more details on the functionality of the first method, see [here](https://arxiv.org/pdf/1207.4525.pdf). The 2nd algorithm implements an efficient approximation of the Hungarian Algorithm, while the 3rd one implements an efficient, heuristic solution to the assignment problem in unbalanced bipartite graphs.

### Similarity Join
Similarity Join conveys the state-of-the-art algorithms for accelerating the computation of a specific character- or token-based similarity measure in combination with a user-determined similarity threshold.

The following token-based similarity jon algorithms are supported:
  1) AllPairs
  2) PPJoin
  3) SilkMoth

The following character-based similarity jon algorithms are also supported:
  1) FastSS
  2) PassJoin
  3) PartEnum
  4) EdJoin
  5) AllPairs

### Comparison Prioritization
Comparison Prioritization associates all comparisons in a block collection with a weight that is proportional to the likelihood that they involve duplicates and then, it emits them iteratively, in decreasing weight.

The following methods are currently supported:
  1) Local Progressive Sorted Neighborhood
  2) Global Progressive Sorted Neighborhood
  3) Progressive Block Scheduling
  4) Progressive Entity Scheduling
  5) Progressive Global Top Comparisons
  6) Progressive Local Top Comparisons

For more details on the functionality of these methods, see [here](https://arxiv.org/pdf/1905.06385.pdf).
  
## How to add JedAI as a dependency to your project

Visit https://search.maven.org/artifact/org.scify/jedai-core

## How to run JedAI as a Docker image

After installing Docker on your machine, type the following commands:

~~~~
docker pull gmandi/jedai-webapp

docker run -p 8080:8080 gmandi/jedai-webapp
~~~~

Then, open your browser and go to localhost:8080. JedAI should be running on your browser!

## How to use JedAI with Python

You can combine JedAI with Python through PyJNIus (https://github.com/kivy/pyjnius).

Preparation Steps:
1. Install python3 and PyJNIus (https://github.com/kivy/pyjnius).
2. Install java 8 openjdk and openjfx for java 8 and configure it as the default java.
3. Create a directory or a jar file with jedai-core and its dependencies. One approach is to use the maven-assembly-plugin
(https://maven.apache.org/plugins/maven-assembly-plugin/usage.html), which will package everything to a single jar file:
jedai-core-3.0-jar-with-dependencies.jar

In the following code block a simple example is presented in python 3. The code reads the ACM.csv file found at (JedAIToolkit/data/cleanCleanErDatasets/DBLP-ACM) and prints the entities found:

~~~~
import jnius_config;
jnius_config.add_classpath('jedai-core-3.0-jar-with-dependencies.jar')

from jnius import autoclass

filePath = 'path_to/ACM.csv'
CsvReader = autoclass('org.scify.jedai.datareader.entityreader.EntityCSVReader')
List = autoclass('java.util.List')
EntityProfile = autoclass('org.scify.jedai.datamodel.EntityProfile')
Attribute = autoclass('org.scify.jedai.datamodel.Attribute')
csvReader = CsvReader(filePath)
csvReader.setAttributeNamesInFirstRow(True);
csvReader.setSeparator(",");
csvReader.setIdIndex(0);
profiles = csvReader.getEntityProfiles()
profilesIterator = profiles.iterator()
while profilesIterator.hasNext() :
    profile = profilesIterator.next()
    print("\n\n" + profile.getEntityUrl())
    attributesIterator = profile.getAttributes().iterator()
    while attributesIterator.hasNext() :
        print(attributesIterator.next().toString())
~~~~

OntopStream extension
=====

OntopStream is a research protype extension of the popular Ontology-Based Data Access engine [Ontop](https://ontop-vkg.org), for performing streaming semantical data access of relational data streams coming from heterogeneous data sources (Kafka, Kinesis, Hive, JDBC DBs, filesystems) ingested in Flink as dynamic tables. OntopStream is part of the [__Chimera suite__](https://chimera-suite.github.io/chimera/) project. The interaction between OntopStream and the Flink Stream Processing engine is possible thanks to a custom extension of the third-party [Flink JDBC driver](https://github.com/ververica/flink-jdbc-driver).

The Streaming Virtual Knowledge Graph approach of OntopStream provides a common semantic data access layer that abstracts the specific aspects of the streaming data sources, allowing to completely automate the query translations between RSP-QL and the mapped FlinkSQL streaming relational sources.

Users can perform queries on their local machine using the ___OntopStream-CLI___, or remotely by sending the query requests to the ___OntopStream-Endpoint___ (provided as a docker image). Jupyter notebooks can be used to query the remote endpoint using the [SPARQLWrapper](https://sparqlwrapper.readthedocs.io/en/latest/) open-source library. This allows to further analyze the query results in the notebook using the most popular data science libraries.

#### Links
- [Releases](https://github.com/chimera-suite/OntopStream/releases)
- [DockerHub](https://hub.docker.com/r/chimerasuite/ontop-stream)
- [Tutorial (docker)](https://github.com/chimera-suite/OntopStream-running-example)
- [__NEW__] [WWW Conference Tutorial](https://github.com/chimera-suite/OntopStream-TheWebConf2022Tutorial) - [event](https://streamreasoning.org/events/web-stream-processing-tutorial-thewebconf/)
- Tutorial (cli): coming soon
- [Performance evaluation](https://github.com/chimera-suite/OntopStream-evaluation)

Ontop
--------------------

Ontop is a Virtual Knowledge Graph system.
It exposes the content of arbitrary relational databases as knowledge graphs. These graphs are virtual, which means that data remains in the data sources instead of being moved to another database.

Ontop translates [SPARQL queries](https://www.w3.org/TR/sparql11-query/) expressed over the knowledge graphs into SQL queries executed by the relational data sources. It relies on [R2RML mappings](https://www.w3.org/TR/r2rml/) and can take advantage of lightweight ontologies.

#### Links
- [Official Website and Documentation](https://ontop-vkg.org)
- [GitHub](https://github.com/ontop/ontop/)

Usage & Troubleshooting
--------------------
### CLI
To run OntopStream-CLI, you need to execute the `ontop.sh` script and use the following syntax:

```
./ontop query                               \
      --ontology=<path-owl>                 \
      --mapping=<path-obda-mappings>        \
      --properties=<path-properties-file>   \
      --query=<path-query-file>             \
      --output=<optional-results-file>
```
Where the the OntopStream’s arguments are respectively:

1. `ontology`: the path to the file containing the ontological concepts needed by the Ontop reasoner for describing the semantic of the relational data stored in the data lake.
2. `mapping`: the path to the mapping file for the RSP-QL to FlinkSQL translations performed by the Streaming Virtual Knowledge Graph mechanism of OntopStream.
3. `properties`: the path to the configuration file for correctly instantiating the JDBC connection to the Apache Spark query engine. In particular, the file must contain the address of the Flink JDBC endpoint.
4. `query`: the path to the file containing the RSP-QL query to be submitted. (Note: if are not needed windows conditions, you can just write a SPARQL query).
5. `output`[OPTIONAL]: The path to a file, created by OntopStream during its execution, where the results are stored in CSV format. If not specified, the results are printed on the terminal.

To terminate the query execution, you can use `CTRL+C`. The query de-registration in Flink is automated.

#### Troubleshooting

If during the CLI execution you get a message similar to this one

```
Exception in thread "main" java.lang.UnsupportedClassVersionError: it/unibz/inf/ontop/injection/OntopReformulationConfiguration has been compiled by
a more recent version of the Java Runtime (class file version 55.0), this version of the Java Runtime only recognizes class file versions up to 52.0
	at java.lang.ClassLoader.defineClass1(Native Method)
	at java.lang.ClassLoader.defineClass(ClassLoader.java:756)
    ...
```
There is a problem with the default Java version used in your machine. Please make sure to have installed [JDK-11](http://jdk.java.net/java-se-ri/11) or [OpenJDK-11](https://jdk.java.net/archive/).

If not, you can download Java 11 and set the right version with `export JAVA_HOME='<PATH-TO-JDK-INSTALLATION-FOLDER>'` (Linux machines)


Compiling, packing, etc.
--------------------

__NOTE__: there are available a set of pre-built __[OntopStream distributions](https://github.com/chimera-suite/OntopStream/releases)__. You don't need to compile yourself the code (unless you want to change something...)

The project is a [Maven](http://maven.apache.org/) project. Compiling,
running the unit tests, building the release binaries all can be done
using maven.  Currently, we use Maven 3 and Java 11 to build the
project.

Here are briefly detailed the steps for compiling OntopStream:


1. Download and install [JDK-11](http://jdk.java.net/java-se-ri/11) or [OpenJDK-11](https://jdk.java.net/archive/)


2. Set the env variable `$JAVA_HOME` to the JDK installation path

```
export JAVA_HOME='<PATH-TO-JDK-FOLDER>'
```
Note: You can use `echo $JAVA_HOME` to check the configuration.


4. Build the release by running

```
./build-release.sh
````
You should see a similar ouput in the terminal (after `$ java -version` and `$ ./mvnw -version` you should see `jdk 11` as version)

```
/usr/bin/java
$ java -version
openjdk version "11.0.12" 2021-07-20
OpenJDK Runtime Environment (build 11.0.12+7-Ubuntu-0ubuntu3)
OpenJDK 64-Bit Server VM (build 11.0.12+7-Ubuntu-0ubuntu3, mixed mode, sharing)

$ ./mvnw -version
Apache Maven 3.6.3 (cecedd343002696d0abb50b32b541b8a6ba2883f)
Maven home: /home/matbelcao/.m2/wrapper/dists/apache-maven-3.6.3-bin/1iopthnavndlasol9gbrbg6bf2/apache-maven-3.6.3
Java version: 11, vendor: Oracle Corporation, runtime: /usr/lib/jvm/jdk-11
Default locale: it_IT, platform encoding: UTF-8
OS name: "linux", version: "5.13.0-21-generic", arch: "amd64", family: "unix"

$ git --version
git version 2.32.0


=========================================
 Starting Ontop build script ...
-----------------------------------------

=========================================
 Cleaning                                
-----------------------------------------

=========================================
 Compiling                               
-----------------------------------------

<<<several building messages (may take some minutes)>>>

=========================================
 Done.                                   
-----------------------------------------
```
Then, you can check the outputs in the `build/distribution` folder.


License
-------

The OntopStream framework is available under the Apache License, Version 2.0

```
  Copyright (C) 2021 Chimera suite

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
```


The Ontop framework is available under the Apache License, Version 2.0

```
  Copyright (C) 2009 - 2020 Free University of Bozen-Bolzano

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
```

All documentation is licensed under the
[Creative Commons](http://creativecommons.org/licenses/by/4.0/)
(attribute)  license.

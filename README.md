###Run project :

##### 1) build project using maven;

##### 2) run StreamProducer :

 `java -jar StreamProducer/target/StreamProducer-1.0-SNAPSHOT.jar  /Users/andrey/IdeaProjects/StreamCombiner/StreamProducer/src/main/resources`
  
where :

`StreamProducer/target/StreamProducer-1.0-SNAPSHOT.jar` - path to jar-file

and

`/Users/andrey/IdeaProjects/StreamCombiner/StreamProducer/src/main/resources` - path to folder that contains file `servers.txt` with information about StreamProducers (files - `server1.txt ... server3.txt` that located in the same directory).

other variant to run StreamProducer :

`java -jar StreamProducer/target/StreamProducer-1.0-SNAPSHOT.jar` - in this way the application uses build-in servers.txt.

##### 3) run StreamCombinerClient :

`java -jar StreamCombinerClient/target/StreamCombinerClient-1.0-SNAPSHOT-jar-with-dependencies.jar /Users/andrey/IdeaProjects/StreamCombiner/StreamCombinerClient/src/main/resources`

where :

`StreamCombinerClient/target/StreamCombinerClient-1.0-SNAPSHOT-jar-with-dependencies.jar` - path to jar-file that contains all dependencies

and

`/Users/andrey/IdeaProjects/StreamCombiner/StreamCombinerClient/src/main/resources` -
path to folder with servers.txt that contains information about StreamProducers (hosts and ports of every StreamProducer)   

other variant to run StreamCombinerClient :

`java -jar StreamCombinerClient/target/StreamCombinerClient-1.0-SNAPSHOT-jar-with-dependencies.jar` - in this way the application uses build-in servers.txt.

#### Tests :

Main tests localted in com.client.combiner.StreamCombinerTest
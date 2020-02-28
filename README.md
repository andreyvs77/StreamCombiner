###Run project :

##### 1) build project using maven;

##### 2) run StreamProducer :

 `java -jar StreamProducer/target/StreamProducer-1.0-SNAPSHOT.jar  /home/aserkes/IdeaProjects/StreamCombiner/StreamProducer/src/main/resources/`
  
where :

`StreamProducer/target/StreamProducer-1.0-SNAPSHOT.jar` - path to jar-file

and

`/home/aserkes/IdeaProjects/StreamCombiner/StreamProducer/src/main/resources/` - path to file `servers.txt` that contains information about StreamProducers (files - `server1.txt ... server3.txt` that located in the same directory). 

##### 3) run StreamCombinerClient :

`java -jar StreamCombinerClient/target/StreamCombinerClient-1.0-SNAPSHOT-jar-with-dependencies.jar /Users/andrey/IdeaProjects/StreamCombiner/StreamCombinerClient/src/main/resources/`

where :

`StreamCombinerClient/target/StreamCombinerClient-1.0-SNAPSHOT-jar-with-dependencies.jar` - path to jar-file that contains all dependencies

and

`/Users/andrey/IdeaProjects/StreamCombiner/StreamCombinerClient/src/main/resources/` -
path to file servers.txt that contains information about StreamProducers (hosts and ports of every StreamProducer)   

#### Tests :

Main tests localted in com.client.combiner.StreamCombinerTest
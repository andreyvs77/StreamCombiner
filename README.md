mvn clean install

mvn --projects StreamProducer exec:java
or
java -jar StreamProducer/target/StreamProducer-1.0-SNAPSHOT.jar

and then 

mvn --projects StreamCombinerClient exec:java
or
java -jar StreamCombinerClient/target/StreamCombinerClient-1.0-SNAPSHOT-jar-with-dependencies.jar /Users/andrey/IdeaProjects/StreamCombiner/StreamCombinerClient/src/main/resources/
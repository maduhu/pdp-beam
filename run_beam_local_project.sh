mvn compile -X exec:java -Dexec.mainClass=br.ufrgs.inf.beam.examples.WordCount \
    -Dexec.args="--inputFile=pom.xml --output=counts --runner=DirectRunner"

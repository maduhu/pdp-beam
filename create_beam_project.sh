mvn archetype:generate \
    -DarchetypeRepository=https://repository.apache.org/content/groups/snapshots \
    -DarchetypeGroupId=org.apache.beam \
    -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
    -DarchetypeVersion=LATEST \
    -DgroupId=br.ufrgs.inf.beam \
    -DartifactId=gppd-beam \
    -Dversion="0.1" \
    -Dpackage=br.ufrgs.inf.beam.examples \
    -DinteractiveMode=false

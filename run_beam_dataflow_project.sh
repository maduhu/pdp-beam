mvn compile -X exec:java -Dexec.mainClass=br.ufrgs.inf.beam.examples.WordCount \
    -Dexec.args="--project=gppd-beam --output=gs://gppd-beam-bucket/output --gcpTempLocation=gs://gppd-beam-bucket/tmp --runner=DataflowRunner"

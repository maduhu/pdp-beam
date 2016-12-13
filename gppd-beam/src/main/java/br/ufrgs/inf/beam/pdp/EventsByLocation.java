package br.ufrgs.inf.beam.pdp;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EventsByLocation {

    private static final Logger LOG = LoggerFactory.getLogger(EventsByLocation.class);

    private interface Options extends PipelineOptions {
        String GDELT_EVENTS_URL = "http://data.gdeltproject.org/events/";

        @Description("GDELT file date")
        @Default.InstanceFactory(GDELTFileFactory.class)
        String getDate();
        void setDate(String value);

        @Description("Input Path")
        String getInput();
        void setInput(String value);

        @Description("Output Path")
        String getOutput();
        void setOutput(String value);

        class GDELTFileFactory implements DefaultValueFactory<String> {
            public String create(PipelineOptions options) {
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
                return format.format(new Date());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        if (options.getOutput() == null) {
            options.setOutput("/tmp/gdelt-" + options.getDate());
        }
        LOG.info(options.toString());

        Pipeline pipeline = Pipeline.create(options);
        pipeline
                .apply("GDELTFile", TextIO.Read.from(options.getInput()))
                .apply("ExtractLocation", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String[] fields = c.element().split("\\t+");
                        if (fields.length > 22) {
                            if (fields[21].length() > 2) {
                                c.output(fields[21].substring(0, 1));
                            } else {
                                c.output(fields[21]);
                            }
                        } else {
                            c.output("NA");
                        }
                    }
                }))
                .apply("Filtering", Filter.by(new SerializableFunction<String, Boolean>() {
                    public Boolean apply(String input) {
                        if (input.equals("NA")) {
                            return false;
                        }
                        if (input.startsWith("-")) {
                            return false;
                        }
                        if (input.length() != 2) {
                            return false;
                        }
                        return true;
                    }
                }))
                .apply("CountPerLocation", Count.<String>perElement())
                .apply("StringFormat", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                }))
                .apply("Results", TextIO.Write.to(options.getOutput()));

        pipeline.run();
    }

}

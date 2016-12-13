package br.ufrgs.inf.beam.pdp;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.examples.common.ExampleBigQueryTableOptions;
import org.apache.beam.examples.common.ExampleOptions;
import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TrafficMaxLaneFlow {

    static final int WINDOW_DURATION = 60;  // Default sliding window duration in minutes
    static final int WINDOW_SLIDE_EVERY = 5;  // Default window 'slide every' setting in minutes

    @DefaultCoder(AvroCoder.class)
    static class LaneInfo {
        @Nullable String stationId;
        @Nullable String lane;
        @Nullable String direction;
        @Nullable String freeway;
        @Nullable String recordedTimestamp;
        @Nullable Integer laneFlow;
        @Nullable Integer totalFlow;
        @Nullable Double laneAO;
        @Nullable Double laneAS;

        public LaneInfo() {}

        public LaneInfo(String stationId, String lane, String direction, String freeway,
                        String timestamp, Integer laneFlow, Double laneAO,
                        Double laneAS, Integer totalFlow) {
            this.stationId = stationId;
            this.lane = lane;
            this.direction = direction;
            this.freeway = freeway;
            this.recordedTimestamp = timestamp;
            this.laneFlow = laneFlow;
            this.laneAO = laneAO;
            this.laneAS = laneAS;
            this.totalFlow = totalFlow;
        }

        public String getStationId() {
            return this.stationId;
        }
        public String getLane() {
            return this.lane;
        }
        public String getDirection() {
            return this.direction;
        }
        public String getFreeway() {
            return this.freeway;
        }
        public String getRecordedTimestamp() {
            return this.recordedTimestamp;
        }
        public Integer getLaneFlow() {
            return this.laneFlow;
        }
        public Double getLaneAO() {
            return this.laneAO;
        }
        public Double getLaneAS() {
            return this.laneAS;
        }
        public Integer getTotalFlow() {
            return this.totalFlow;
        }
    }

    static class ExtractTimestamps extends DoFn<String, String> {
        private static final DateTimeFormatter dateTimeFormat =
                DateTimeFormat.forPattern("MM/dd/yyyy HH:mm:ss");

        @ProcessElement
        public void processElement(DoFn<String, String>.ProcessContext c) throws Exception {
            String[] items = c.element().split(",");
            if (items.length > 0) {
                try {
                    String timestamp = items[0];
                    c.outputWithTimestamp(c.element(), new Instant(dateTimeFormat.parseMillis(timestamp)));
                } catch (IllegalArgumentException e) {
                }
            }
        }
    }

    static class ExtractFlowInfoFn extends DoFn<String, KV<String, LaneInfo>> {

        @ProcessElement
        public void processElement(ProcessContext c) {
            String[] items = c.element().split(",");
            if (items.length < 48) {
                return;
            }

            String timestamp = items[0];
            String stationId = items[1];
            String freeway = items[2];
            String direction = items[3];
            Integer totalFlow = tryIntParse(items[7]);
            for (int i = 1; i <= 8; ++i) {
                Integer laneFlow = tryIntParse(items[6 + 5 * i]);
                Double laneAvgOccupancy = tryDoubleParse(items[7 + 5 * i]);
                Double laneAvgSpeed = tryDoubleParse(items[8 + 5 * i]);
                if (laneFlow == null || laneAvgOccupancy == null || laneAvgSpeed == null) {
                    return;
                }
                LaneInfo laneInfo = new LaneInfo(stationId, "lane" + i, direction, freeway, timestamp,
                        laneFlow, laneAvgOccupancy, laneAvgSpeed, totalFlow);
                c.output(KV.of(stationId, laneInfo));
            }
        }
    }

    public static class MaxFlow implements SerializableFunction<Iterable<LaneInfo>, LaneInfo> {
        @Override
        public LaneInfo apply(Iterable<LaneInfo> input) {
            Integer max = 0;
            LaneInfo maxInfo = new LaneInfo();
            for (LaneInfo item : input) {
                Integer flow = item.getLaneFlow();
                if (flow != null && (flow >= max)) {
                    max = flow;
                    maxInfo = item;
                }
            }
            return maxInfo;
        }
    }

    static class FormatMaxesFn extends DoFn<KV<String, LaneInfo>, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {

            LaneInfo laneInfo = c.element().getValue();
            TableRow row = new TableRow()
                    .set("station_id", c.element().getKey())
                    .set("direction", laneInfo.getDirection())
                    .set("freeway", laneInfo.getFreeway())
                    .set("lane_max_flow", laneInfo.getLaneFlow())
                    .set("lane", laneInfo.getLane())
                    .set("avg_occ", laneInfo.getLaneAO())
                    .set("avg_speed", laneInfo.getLaneAS())
                    .set("total_flow", laneInfo.getTotalFlow())
                    .set("recorded_timestamp", laneInfo.getRecordedTimestamp())
                    .set("window_timestamp", c.timestamp().toString());
            c.output(row);
        }

        static TableSchema getSchema() {
            List<TableFieldSchema> fields = new ArrayList<>();
            fields.add(new TableFieldSchema().setName("station_id").setType("STRING"));
            fields.add(new TableFieldSchema().setName("direction").setType("STRING"));
            fields.add(new TableFieldSchema().setName("freeway").setType("STRING"));
            fields.add(new TableFieldSchema().setName("lane_max_flow").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("lane").setType("STRING"));
            fields.add(new TableFieldSchema().setName("avg_occ").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("avg_speed").setType("FLOAT"));
            fields.add(new TableFieldSchema().setName("total_flow").setType("INTEGER"));
            fields.add(new TableFieldSchema().setName("window_timestamp").setType("TIMESTAMP"));
            fields.add(new TableFieldSchema().setName("recorded_timestamp").setType("STRING"));
            TableSchema schema = new TableSchema().setFields(fields);
            return schema;
        }
    }

    static class MaxLaneFlow
            extends PTransform<PCollection<KV<String, LaneInfo>>, PCollection<TableRow>> {
        @Override
        public PCollection<TableRow> expand(PCollection<KV<String, LaneInfo>> flowInfo) {
            PCollection<KV<String, LaneInfo>> flowMaxes =
                    flowInfo.apply(Combine.<String, LaneInfo>perKey(
                            new MaxFlow()));

            PCollection<TableRow> results = flowMaxes.apply(
                    ParDo.of(new FormatMaxesFn()));

            return results;
        }
    }

    static class ReadFileAndExtractTimestamps extends PTransform<PBegin, PCollection<String>> {
        private final String inputFile;

        public ReadFileAndExtractTimestamps(String inputFile) {
            this.inputFile = inputFile;
        }

        @Override
        public PCollection<String> expand(PBegin begin) {
            return begin
                    .apply(TextIO.Read.from(inputFile))
                    .apply(ParDo.of(new ExtractTimestamps()));
        }
    }

    private interface TrafficMaxLaneFlowOptions extends ExampleOptions, ExampleBigQueryTableOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/traffic_sensor/"
                + "Freeways-5Minaa2010-01-01_to_2010-02-15.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Numeric value of sliding window duration, in minutes")
        @Default.Integer(WINDOW_DURATION)
        Integer getWindowDuration();
        void setWindowDuration(Integer value);

        @Description("Numeric value of window 'slide every' setting, in minutes")
        @Default.Integer(WINDOW_SLIDE_EVERY)
        Integer getWindowSlideEvery();
        void setWindowSlideEvery(Integer value);
    }

    public static void main(String[] args) throws IOException {
        TrafficMaxLaneFlowOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(TrafficMaxLaneFlowOptions.class);
        options.setBigQuerySchema(FormatMaxesFn.getSchema());

        ExampleUtils exampleUtils = new ExampleUtils(options);
        exampleUtils.setup();

        Pipeline pipeline = Pipeline.create(options);
        TableReference tableRef = new TableReference();
        tableRef.setProjectId(options.getProject());
        tableRef.setDatasetId(options.getBigQueryDataset());
        tableRef.setTableId(options.getBigQueryTable());

        pipeline
                .apply("ReadLines", new ReadFileAndExtractTimestamps(options.getInputFile()))
                .apply(ParDo.of(new ExtractFlowInfoFn()))
                .apply(Window.<KV<String, LaneInfo>>into(SlidingWindows.of(
                        Duration.standardMinutes(options.getWindowDuration())).
                        every(Duration.standardMinutes(options.getWindowSlideEvery()))))
                .apply(new MaxLaneFlow())
                .apply(BigQueryIO.Write.to(tableRef)
                        .withSchema(FormatMaxesFn.getSchema()));

        PipelineResult result = pipeline.run();

        exampleUtils.waitToFinish(result);
    }

    private static Integer tryIntParse(String number) {
        try {
            return Integer.parseInt(number);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Double tryDoubleParse(String number) {
        try {
            return Double.parseDouble(number);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}

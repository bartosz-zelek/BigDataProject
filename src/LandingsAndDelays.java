import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

// Q&A
// 1. Czy brać pod uwagę jedynie dodatnie opóźnienia? TAK MAX(0, ARRIVAL_DELAY)
// 2. Jak uwzględnić brakujące wartości w kolumnie ARRIVAL_DELAY? - jeżeli CANCELLED=0 to traktuję jako brak opóźnienia -> 0
// 3. Czy brać pod uwagę kolumnę DIVERTED? - nie


public class LandingsAndDelays extends Configured implements Tool{


    // <offset in file, line from file> -> <"airportID,year,month", delay>
    public static class LandsingsAndDelaysMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final int YEAR = 0; // valid 100%
        private static final int MONTH = 1; // valid 100%
        private static final int DESTINATION_AIRPORT = 8; // AIRPORT ID - valid 100%
        private static final int ARRIVAL_DELAY = 22; // valid 98% - 105k brakujących wartości
        private static final int DIVERTED = 23; // valid 100% - jeżeli 1 to dane są losowo nieuzupełnione
        private static final int CANCELLED = 24; // valid 100% - 90k anulowanych lotów

        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable();

        @Override
        public void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] columns = line.toString().split(",", -1);
            if (columns.length != 31){
                System.out.println("Invalid record(!=31 records): " + line.toString());
                return;
            }
            if (columns[CANCELLED].equals("1")) return;

            // zgodnie z zaleceniem prowadzącego ignoruję kolumnę DIVERTED
            // gdy wartość ARRIVAL_DELAY jest pusta, to traktuję to jako brak opóźnienia -> 0
            // if (columns[DIVERTED].equals("1")) return;

            int arrivalDelay = 0;
            if (!columns[ARRIVAL_DELAY].isEmpty()) {
                // zgodnie z zaleceniem prowadzącego ujemne wartości ARRIVAL_DELAY są traktowane jako brak opóźnienia -> 0
                arrivalDelay = Math.max(0, Integer.parseInt(columns[ARRIVAL_DELAY]));
            }

            outputKey.set(columns[DESTINATION_AIRPORT] + "," + columns[YEAR] + "," + columns[MONTH]);
            outputValue.set(arrivalDelay);
            context.write(outputKey, new Text("1,"+outputValue));
        }
    }

    // <"airportID,year,month", "1,delay"> -> <"airportID,year,month", "numberOfLandings,delaysSum">
    // counting number of landings by incrementing
    public static class LandingsAndDelaysCombiner extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            int delaysSum = 0;
            int numberOfLandings = 0;
            for (Text value : values) {
                String[] valuesArray = value.toString().split(",");
                delaysSum += Integer.parseInt(valuesArray[1]);
                numberOfLandings++;
            }
            context.write(key, new Text(numberOfLandings + "," + delaysSum));
        }
    }


    // <"airportID,year,month", "numberOfLandings,delaysSum"> -> <"airportID,year,month", "numberOfLandings,delaysSum">
    // counting number of landings by summing counters
    public static class LandsingsAndDelaysReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int delaysSum = 0;
            int numberOfLandings = 0;

            for (Text value : values) {
                String[] valuesArray = value.toString().split(",");
                numberOfLandings += Integer.parseInt(valuesArray[0]);
                delaysSum += Integer.parseInt(valuesArray[1]);
            }

            context.write(key, new Text(numberOfLandings + "," + delaysSum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "LandingsAndDelays");
        job.setJarByClass(this.getClass());

        job.setMapperClass(LandsingsAndDelaysMapper.class);
        job.setReducerClass(LandsingsAndDelaysReducer.class);
        job.setCombinerClass(LandingsAndDelaysCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(3);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[1]));
        int exitCode = ToolRunner.run(new LandingsAndDelays(), args);
        System.exit(exitCode);
    }
}

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

// PYTANIA
// 1. Czy brać pod uwagę jedynie dodatnie opóźnienia?
// 2. Jak uwzględnić brakujące wartości w kolumnie ARRIVAL_DELAY?
// 3. Czy format wyjściowy jest poprawny? AMA,2015,12	237,493.0


public class LandingsAndDelays extends Configured implements Tool{


    public static class LandsingsAndDelaysMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final int YEAR = 0; // valid 100%
        private static final int MONTH = 1; // valid 100%
        private static final int DESTINATION_AIRPORT = 8; // AIRPORT ID - valid 100%
        private static final int ARRIVAL_DELAY = 22; // valid 98% - 105k brakujących wartości
        private static final int DIVERTED = 23; // valid 100% - jeżeli 1 to dane są losowo nieuzupełnione
        private static final int CANCELLED = 24; // valid 100% - 90k anulowanych lotów

        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable();

        @Override
        protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
            String[] columns = line.toString().split(",", -1);
            if (columns.length != 31){
                System.out.println("Invalid record(!=31 records): " + line.toString());
                return;
            }
            if (columns[CANCELLED].equals("1")) return;

            if (columns[DIVERTED].equals("1")) return;

            if (columns[ARRIVAL_DELAY].isEmpty()) {
                System.out.println("Invalid record(ARRIVAL_DELAY is empty but CANCELLED=0 and not DIVERTED): " + line.toString());
                return;
            }

            int arrivalDelay = 0;
            if (!columns[ARRIVAL_DELAY].isEmpty()) {
                arrivalDelay = Integer.parseInt(columns[ARRIVAL_DELAY]);
            }

            outputKey.set(columns[DESTINATION_AIRPORT] + "," + columns[YEAR] + "," + columns[MONTH]);
            outputValue.set(arrivalDelay);
            context.write(outputKey, outputValue);
        }
    }

    public static class LandsingsAndDelaysReducer extends Reducer<Text, IntWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            double delaysSum = 0;
            int numberOfLandings = 0;
            for (IntWritable value : values) {
                delaysSum += value.get();
                numberOfLandings++;
            }
            context.write(key, new Text(numberOfLandings + "," + delaysSum  ));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "LandingsAndDelays");
        job.setJarByClass(this.getClass());

        job.setMapperClass(LandsingsAndDelaysMapper.class);
        job.setReducerClass(LandsingsAndDelaysReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(args[1]));
        int exitCode = ToolRunner.run(new LandingsAndDelays(), args);
        System.exit(exitCode);
    }
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class InvertedIndex {

    public static Set<String> stopWords = new HashSet<>();

    public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outVal = new Text("1");
        private String fileName;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            Path stopPath = new Path("/ex2/stopword/stop_words_eng.txt");
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopPath)));
            String line;
            while ((line = br.readLine()) != null) {
                stopWords.add(line.trim().toLowerCase());
            }
            br.close();

            FileSplit split = (FileSplit) context.getInputSplit();
            fileName = split.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            line = line.replaceAll("[^a-z ]", " ");
            String[] words = line.split("\\s+");
            for (String w : words) {
                if (w.length() > 0 && !stopWords.contains(w)) {
                    outKey.set(w + ":" + fileName);
                    context.write(outKey, outVal);
                }
            }
        }
    }

    public static class IndexCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (Text v : values) {
                sum += Integer.parseInt(v.toString());
            }
            String[] arr = key.toString().split(":");
            context.write(new Text(arr[0]), new Text(arr[1] + ":" + sum));
        }
    }

    public static class IndexReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            int total = 0;
            for (Text v : values) {
                String[] tmp = v.toString().split(":");
                String fname = tmp[0];
                int cnt = Integer.parseInt(tmp[1]);
                sb.append("<").append(fname).append(",").append(cnt).append(">;");
                total += cnt;
            }
            sb.append("<total,").append(total).append(">.");
            context.write(key, new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(IndexCombiner.class);
        job.setReducerClass(IndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

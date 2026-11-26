import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.Arrays;

public class PageRank {
    private static final double DAMPING = 0.85;
    private static final int ITERATIONS = 10; 
    // 第一次Map：解析输入，初始化PR值为1.0
    public static class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String page = parts[0];
            String links = parts.length > 1 ? parts[1] : "";
            context.write(new Text(page), new Text("1.0\t" + links));
        }
    }
    // 迭代Map：分发PR值
    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String page = parts[0];
            double pr = Double.parseDouble(parts[1]);
            String[] links = parts.length > 2 ? parts[2].split(",") : new String[0];
            context.write(new Text(page), new Text("|" + (parts.length > 2 ? parts[2] : "")));
            if (links.length > 0) {
                double share = pr / links.length;
                for (String link : links) {
                    context.write(new Text(link), new Text(String.valueOf(share)));
                }
            }
        }
    }
    // 迭代Reduce：计算新的PR值
    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            String links = "";
            
            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("|")) {
                    links = v.substring(1); // 提取链接列表
                } else {
                    sum += Double.parseDouble(v);
                }
            }
            
            // PageRank 公式: PR = (1-d)/N + d * sum
            // 简化版：PR = (1-d) + d * sum
            double newPR = (1 - DAMPING) + DAMPING * sum;
            
            context.write(key, new Text(String.format("%.10f\t%s", newPR, links)));
        }
    }

    // 最终输出Map
    public static class FinalMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            String page = parts[0];
            double pr = Double.parseDouble(parts[1]);
            context.write(new Text(page), new DoubleWritable(pr));
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String outputBase = args[1];
        Configuration conf = new Configuration();
        // 初始化
        Job initJob = Job.getInstance(conf, "PageRank Init");
        initJob.setJarByClass(PageRank.class);
        initJob.setMapperClass(InitMapper.class);
        initJob.setOutputKeyClass(Text.class);
        initJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(initJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(initJob, new Path(outputBase + "/iter0"));
        initJob.waitForCompletion(true);
        // 迭代10次
        for (int i = 1; i <= ITERATIONS; i++) {
            Job job = Job.getInstance(conf, "PageRank Iter " + i);
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(outputBase + "/iter" + (i-1)));
            FileOutputFormat.setOutputPath(job, new Path(outputBase + "/iter" + i));
            job.waitForCompletion(true);
        }
        
        // 最终格式化输出
        Job finalJob = Job.getInstance(conf, "PageRank Final");
        finalJob.setJarByClass(PageRank.class);
        finalJob.setMapperClass(FinalMapper.class);
        finalJob.setOutputKeyClass(Text.class);
        finalJob.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(finalJob, new Path(outputBase + "/iter" + ITERATIONS));
        FileOutputFormat.setOutputPath(finalJob, new Path(outputBase + "/result"));
        finalJob.waitForCompletion(true);
        
        System.out.println("PageRank 计算完成，结果在 " + outputBase + "/result");
    }
}

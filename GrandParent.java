//第三部分第三题
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GrandParent {

    public static class GPMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if(line.length() == 0) return;
            String[] arr = line.split("\\s+");
            String child = arr[0];
            String parent = arr[1];
            context.write(new Text(parent), new Text("#"+child));
            context.write(new Text(child), new Text(parent));
        }
    }

    public static class GPReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> childrenList = new ArrayList<>();
            List<String> parentList = new ArrayList<>();
            for(Text t : values){
                String s = t.toString();
                if(s.startsWith("#")){
                    childrenList.add(s.substring(1));
                }else{
                    parentList.add(s);
                }
            }
            for(String child : childrenList){
                for(String gp : parentList){
                    context.write(new Text(child), new Text(gp));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"GrandParentJob");
        job.setJarByClass(GrandParent.class);
        job.setMapperClass(GPMapper.class);
        job.setReducerClass(GPReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

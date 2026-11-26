import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PageRankSpark {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("PageRankSpark")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String inputPath = args[0];   
        String outputPath = args[1];  
        int iterations = 10;          
        double damping = 0.85;        
        // 读取输入文件，构建网页邻接关系
        JavaPairRDD<String, List<String>> links = sc.textFile(inputPath)
                .mapToPair(line -> {
                    String[] parts = line.split("\t");
                    String page = parts[0];
                    List<String> neighbors;
                    if (parts.length > 1 && !parts[1].isEmpty()) {
                        neighbors = Arrays.asList(parts[1].split(","));
                    } else {
                        neighbors = new ArrayList<>();
                    }
                    return new Tuple2<>(page, neighbors);
                }).cache();

        // 初始化所有网页的PR值为1.0
        JavaPairRDD<String, Double> ranks = links.mapValues(v -> 1.0);

        // 迭代计算PR值
        for (int i = 0; i < iterations; i++) {
            JavaPairRDD<String, Double> contribs = links.join(ranks)
                    .flatMapToPair(tuple -> {
                        List<Tuple2<String, Double>> result = new ArrayList<>();
                        List<String> neighbors = tuple._2()._1();
                        Double rank = tuple._2()._2();
                        // 无出链的页面不贡献PR
                        if (!neighbors.isEmpty()) {
                            double share = rank / neighbors.size();
                            for (String neighbor : neighbors) {
                                result.add(new Tuple2<>(neighbor, share));
                            }
                        }
                        return result.iterator();
                    });
            ranks = contribs.reduceByKey(Double::sum)
                    .mapValues(sum -> (1 - damping) + damping * sum);
        }
        JavaRDD<String> result = ranks.map(tuple ->
                String.format("(%s,%.10f)", tuple._1(), tuple._2())
        );
        result.saveAsTextFile(outputPath);
        sc.stop();
    }
}

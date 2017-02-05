import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount_decreasing {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("WordCount_decreasing").setMaster("local");	 
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> line_ = jsc.textFile(args[0]);
		JavaRDD<String> word_ = (JavaRDD<String>)( line_.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator()));
		JavaPairRDD<String, Integer> map = word_.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> counts = map.reduceByKey( (count1, count2) -> count1 + count2);
		
		JavaPairRDD<Integer, String> swap_pair = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	           @Override
	           public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
	               return item.swap();
	           }

	        });
		
		JavaPairRDD<Integer, String> sorting=swap_pair.sortByKey(false);
		
		JavaPairRDD<String,Integer> ans = sorting.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
	           @Override
	           public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
	               return item.swap();
	           }

	        });
		ans.saveAsTextFile(args[1]);
		jsc.stop();
	}
}

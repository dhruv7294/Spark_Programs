import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public final class WordCount {
 
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");	 
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("/Users/DS/Downloads/SPARK_HOME/Q2/CleanText.txt");
		JavaRDD<String> words = (JavaRDD<String>)( lines.flatMap(line -> Arrays.asList(SPACE.split(getKeys(line))).iterator()));
		JavaPairRDD<String, Integer> ones = words.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> counts = ones.reduceByKey( (count1, count2) -> count1 + count2);
		//JavaPairRDD<String, Integer> sorted = counts.sortByKey();
		JavaPairRDD<Integer, String> swappedPair = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	           @Override
	           public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
	               return item.swap();
	           }

	        });
		JavaPairRDD<Integer, String> sorted=swappedPair.sortByKey(false);
		JavaPairRDD<String,Integer> swappedPair1 = sorted.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
	           @Override
	           public Tuple2<String, Integer> call(Tuple2<Integer, String> item) throws Exception {
	               return item.swap();
	           }

	        });
		swappedPair1.saveAsTextFile(args[1]);
		sc.stop();
	}
	
	public static String getKeys(String line)	{
	
		String data[]=line.split("\\s+");
		StringBuilder result=new StringBuilder();
        StringBuilder newKey=new StringBuilder();
        if(data.length<2)
            return "";
        for(int i=0;i<data.length-1;i++)  {
            if(data[i].length()>0&&data[i+1].length()>0)    {
                newKey=new StringBuilder();
                if(data[i].compareTo(data[i+1])>0)    {
                    newKey.append(data[i]).append(",").append(data[i+1]);
                }
                else    {
                    newKey.append(data[i+1]).append(",").append(data[i]);
                }
                result.append(newKey).append(" ");
            }
        }
        newKey=new StringBuilder();
        if(data[0].length()>0&&data[data.length-1].length()>0)  {
            if(data[0].compareTo(data[data.length-1])>0)    {
                newKey.append(data[0]).append(",").append(data[data.length-1]);
            }
            else{
                newKey.append(data[data.length-1]).append(",").append(data[0]);
            }
            result.append(newKey).append(" ");
        }
        return new String(result);
	}
}
        
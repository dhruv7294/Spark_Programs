import java.util.Arrays;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public final class WordCount_pairs {
 
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		SparkConf conf = new SparkConf().setAppName("WordCount_pairs").setMaster("local");	 
		JavaSparkContext jsc = new JavaSparkContext(conf);
		JavaRDD<String> line_ = jsc.textFile(args[0]);
		JavaRDD<String> word_ = (JavaRDD<String>)( line_.flatMap(line -> Arrays.asList(SPACE.split(getKeys(line))).iterator()));
		JavaPairRDD<String, Integer> map_ = word_.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> counts = map_.reduceByKey( (count1, count2) -> count1 + count2);
		JavaPairRDD<Integer, String> swap_pair = counts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
	           @Override
	           public Tuple2<Integer, String> call(Tuple2<String, Integer> data) throws Exception {
	               return data.swap();
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
	
	public static String getKeys(String line)	{
	
		String values[]=line.split("\\s+");
		StringBuilder res=new StringBuilder();
        StringBuilder key=new StringBuilder();
        if(values.length<2)
            return "";
        for(int i=0;i<values.length-1;i++)  {
            if(values[i].length()>0&&values[i+1].length()>0)    {
                key=new StringBuilder();
                if(values[i].compareTo(values[i+1])>0)    {
                    key.append(values[i]).append(",").append(values[i+1]);
                }
                else    {
                    key.append(values[i+1]).append(",").append(values[i]);
                }
                res.append(key).append(" ");
            }
        }
        key=new StringBuilder();
        if(values[0].length()>0&&values[values.length-1].length()>0)  {
            if(values[0].compareTo(values[values.length-1])>0)    {
                key.append(values[0]).append(",").append(values[values.length-1]);
            }
            else{
                key.append(values[values.length-1]).append(",").append(values[0]);
            }
            res.append(key).append(" ");
        }
        return new String(res);
	}
}
        
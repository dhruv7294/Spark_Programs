import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class Voting {

	public static void main(String[] args){

		SparkConf conf = new SparkConf().setAppName("Voting").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		String csvInput = args[0];
    
		JavaRDD<String> csvData = sc.textFile(csvInput, 1);
		String head=csvData.first();
		JavaRDD<String> textFromFile = csvData.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return !s.equalsIgnoreCase(head);
            }
        });	
		

		JavaPairRDD<String,Integer> dist = textFromFile.mapToPair(
		  new PairFunction<String, String,Integer>() {
		      public Tuple2 call(String line) throws Exception {

		         String[] column = line.split(",");
		         return new Tuple2(column[2],Integer.parseInt(column[3]));
		      }
		});

		JavaPairRDD<String, Integer> counts = dist.reduceByKey(
				  new Function2<Integer, Integer, Integer>() {
				    public Integer call(Integer i1, Integer i2) {
				      return i1 + i2;
				    }
				  });
		JavaPairRDD<String, Integer> sorting = counts.sortByKey();
		sorting.saveAsTextFile(args[1]);
		
		JavaRDD<Integer> vote = textFromFile.map(
				new Function<String,Integer>(){
					@Override
					public Integer call(String line) throws Exception {
						String[] column = line.split(",");
						return Integer.parseInt(column[3]);
					}
				});
		double mean_value=vote.reduce((a,b)-> a+b);
		mean_value/=99;
		System.out.println(mean_value);
		List<String> list=new ArrayList<>();
		String content="Mean,"+mean_value;
		list.add(content);
		JavaRDD<String> output=sc.parallelize(list);
		output.saveAsTextFile(args[2]);   //Third Argument for 'Mean' folder
					
		
		sc.stop();
	}
}


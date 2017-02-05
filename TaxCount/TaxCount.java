import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TaxCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TaxCount").setMaster("local[3]");
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
		

		JavaPairRDD<String,Double> dist = textFromFile.mapToPair(
		  new PairFunction<String, String,Double>() {
		      public Tuple2 call(String line) throws Exception {

		         String[] columns = line.split(",");
		         return new Tuple2(columns[1]+columns[3],Double.parseDouble(columns[4]));
		      }
		});

		JavaPairRDD<String, Double> count = dist.reduceByKey(
				  new Function2<Double, Double, Double>() {
				    public Double call(Double i1, Double i2) {
				      return i1 + i2;
				    }
				  });
		JavaPairRDD<String, Double> sorting = count.sortByKey();
		sorting.saveAsTextFile(args[1]);
		
	}

}

class Record{
	String state;
	String level;
	
	public Record(String st,String lev){
		this.state=st;
		this.level=lev;
	}
}
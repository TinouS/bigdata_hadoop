package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TPSpark {

	public static void main(String[] args) throws InterruptedException {
		if (args.length != 0){
		SparkConf conf = new SparkConf().setAppName("TP Spark");
                JavaSparkContext context = new JavaSparkContext(conf);
                JavaRDD<String> rdd;
                JavaPairRDD<String,String> rddTmp;
                JavaPairRDD<String,Double> rddResult;
                rdd = context.textFile(args[0]);
                System.out.println("Number of partitions before repartition: "+rdd.getNumPartitions());
                rdd = rdd.repartition(Integer.parseInt(conf.get("spark.executor.instances")));
                System.out.println("number of entries before filtering: "+rdd.count());
                System.out.println("Number of partitions after repartition: "+rdd.getNumPartitions());
                rddTmp = rdd.keyBy((x)-> {String[] tokens = x.split(",");
                        return tokens[2];
                        });
                rddResult = rddTmp.mapValues((x)->{
                    String[] tokens = x.split(",");
                    if (tokens[4].isEmpty()&& tokens[4].equals("population"))
                        return (double)-1;
                    return Double.parseDouble(tokens[4]);
                });
                rddResult = rddResult.filter((x) -> x._2() != -1);
                System.out.println("number of entries after filtering: "+rddResult.count());
                //Thread.sleep(80000);
                }
	}
	
}

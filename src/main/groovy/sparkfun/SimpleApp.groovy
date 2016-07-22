package sparkfun

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext

public class SimpleApp {
    public static void main(String[] args) {
        def logFile = "/Users/rahul/Downloads/spark-1.6.2/README.md"
        def conf = new SparkConf().setAppName("Simple Application")
        def sc = new JavaSparkContext(conf)
        JavaRDD<String> logData = sc.textFile(logFile).cache()

        long numAs = logData.filter({ String s -> return s.contains("a"); }).count()
        long numBs = logData.filter({ String s -> return s.contains("b"); }).count()

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs)
    }
}

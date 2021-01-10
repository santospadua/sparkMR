package spark.puc.spark.run;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkProgram 
{
    public static void main(String[] args)
    {
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("ATP");
        
        try(JavaSparkContext context = new JavaSparkContext(config))
        {
            JavaRDD<String> file = context.textFile("files/ocorrencias_criminais.csv");
            
            JavaPairRDD<String, Integer> crimesPorAno = file.mapToPair(x ->
            {
                String[] lines = x.split(";");
                return new Tuple2<>(lines[2], 1);
            }).reduceByKey((x, y) -> x + y);
            
            System.out.println(crimesPorAno.collect());
            crimesPorAno.saveAsTextFile("crimesPorAno");
        }
    }
}
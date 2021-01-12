package spark.puc.spark.run;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkProgram 
{
    private static JavaSparkContext context;
    
    public static void main(String[] args)
    {
        SparkConf config = new SparkConf().setMaster("local[*]").setAppName("ATP");
        
        context = new JavaSparkContext(config);
        
        JavaRDD<String> file = context.textFile("files/ocorrencias_criminais.csv");
            
        JavaPairRDD<String, Integer> crimesPorAno = file.mapToPair(x ->
        {
            String[] lines = x.split(";");
            return new Tuple2<>(lines[2], 1);
        }).reduceByKey((x, y) -> x + y);
            
        System.out.println(crimesPorAno.collect());
        crimesPorAno.saveAsTextFile("crimesPorAno");
        
        JavaPairRDD<Tuple2<String, String>, Integer> crimesPorAnoNarcoticos = file.mapToPair(t -> 
        {    
            String[] lines = t.split(";");
            return new Tuple2<>(new Tuple2<>(lines[2], lines[4]), 1); 
        }).filter(f -> f._1()._2().equals("NARCOTICS")).reduceByKey((x, y) -> x + y);
        
        System.out.println(crimesPorAnoNarcoticos.collect());
        crimesPorAnoNarcoticos.saveAsTextFile("crimesPorAnoNarcoticos");
        
        JavaPairRDD<Tuple2<String, String>, Integer> crimesDiasParesNarcoticos = file.mapToPair(x -> 
        {
            String[] lines = x.split(";");
            return new Tuple2<>(new Tuple2<>(lines[2], lines[4]), Integer.parseInt(lines[0]));
        }).filter(f -> f._2() % 2 == 0 && f._1()._2().equals("NARCOTICS")).reduceByKey((x, y) -> x + y);
        
        System.out.println(crimesDiasParesNarcoticos.collect());
        crimesPorAnoNarcoticos.saveAsTextFile("crimesDiasParesNarcoticos");
    }
}
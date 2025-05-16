package tn.insat.project;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class Stream {
    public static void main(String[] args) throws InterruptedException {

        // Configuration Spark
        SparkConf conf = new SparkConf()
                .setAppName("NetworkWordCount")
                .setMaster("local[*]");

        // Contexte de streaming : traitement toutes les 1 seconde
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // Récupérer les données depuis un socket (localhost:9999)
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

        // Séparer les lignes en mots
        JavaDStream<String> words = lines.flatMap((FlatMapFunction<String, String>) line ->
                Arrays.asList(line.split(" ")).iterator());

        // Associer chaque mot à 1
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
                (PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        // Compter le nombre d'occurrences par mot
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
                (Function2<Integer, Integer, Integer>) Integer::sum);

        // Afficher les résultats
        wordCounts.print();

        // Démarrer le stream
        jssc.start();
        jssc.awaitTermination();
    }
}

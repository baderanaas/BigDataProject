package tn.insat.project;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import java.util.*;

public class ResultCount {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("InspectionDateTrend")
                .setMaster("local[*]")
                .set("spark.streaming.blockSize", "64m");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // Paramètres Kafka
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "result-count-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        // Lire depuis le topic inspections_raw
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(Arrays.asList("inspections"), kafkaParams)
                );

        // Extraire la valeur (ligne CSV)
        JavaDStream<String> lines = stream.map(record -> record.value());

        // Extraction du champ "result" (colonne index 13)
        JavaPairDStream<String, Integer> resultCounts = lines
                .filter(line -> {
                    String[] parts = line.split(",");
                    return parts.length > 13 && !parts[13].trim().isEmpty();
                })
                .mapToPair(line -> {
                    String[] parts = line.split(",");
                    String result = parts[13].trim(); // Récupérer le résultat
                    return new Tuple2<>(result, 1);
                })
                .reduceByKey((a, b) -> a + b); // Compter les occurrences

        // Afficher et envoyer vers Kafka
        resultCounts.foreachRDD(rdd -> {
            rdd.foreach(tuple -> {
                String result = tuple._1();
                Integer count = tuple._2();

                // Format attendu côté Python : "<result>-<count>"
                String message = result + "-" + count;

                System.out.println("Résultat : " + result + ", Count : " + count);
                KafkaResultProducer.sendResult("results_summary", result, message);
            });
            if (rdd.getStorageLevel().useMemory()) {
                rdd.unpersist();}
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
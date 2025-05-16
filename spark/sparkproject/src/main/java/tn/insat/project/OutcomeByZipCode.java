package tn.insat.project;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.kafka010.*;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Tuple2;

import java.util.*;

import java.util.Arrays;

public class OutcomeByZipCode {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("InspectionDateTrend")
                .setMaster("local[*]")
                .set("spark.streaming.blockSize", "64m");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

        // Connexion au flux réseau
        // Paramètres Kafka
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "spark-streaming-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("inspections");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> lines = stream.map(record -> record.value());

        // Mapper chaque ligne vers une paire ((street_number, result), 1)
        JavaPairDStream<Tuple2<String, String>, Integer> streetResultPairs = lines.mapToPair(line -> {
            String[] parts = line.split(",");
            String streetNumber = "";
            String result = "";

            if (parts.length > 4) streetNumber = parts[4];   // street_number_from
            if (parts.length > 13) result = parts[13];       // result

            return new Tuple2<>(new Tuple2<>(streetNumber, result), 1);
        });

        // Réduire par clé ((street_number, result)) pour compter les occurrences
        JavaPairDStream<Tuple2<String, String>, Integer> outcomeCounts = streetResultPairs
                .reduceByKey((a, b) -> a + b);

        // Afficher les résultats
        outcomeCounts.foreachRDD(rdd -> {
            rdd.foreach(tuple -> {
                Tuple2<String, String> key = tuple._1();
                Integer count = tuple._2();

                String streetNumber = key._1(); // ex: "1880.0"
                String result = key._2();       // ex: "ReInspection Required"

                // Affichage formaté dans la console
                System.out.printf("[failures_by_location] Reçu : %s - %s - %d%n",
                        streetNumber, result, count);

                // Envoie les trois champs dans Kafka (on garde la clé pour la partition si besoin)
                String message = streetNumber + "-" + result + "-" + count;
                KafkaResultProducer.sendResult("failures_by_location", streetNumber, message);
            });

            if (rdd.getStorageLevel().useMemory()) {
                rdd.unpersist();
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
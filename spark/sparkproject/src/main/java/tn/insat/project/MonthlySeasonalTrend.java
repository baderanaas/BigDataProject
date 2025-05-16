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

public class MonthlySeasonalTrend {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("InspectionDateTrend")
                .setMaster("local[*]")
                .set("spark.streaming.blockSize", "64m");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

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

        JavaDStream<String> monthlyDates = lines.map(line -> {
            String[] parts = line.split(",");
            if (parts.length > 14 && !parts[14].isEmpty()) {
                String dateStr = parts[14];
                return dateStr.substring(0, 7);
            }
            return "";
        });

        JavaPairDStream<String, Integer> monthlyCounts = monthlyDates
                .filter(month -> !month.isEmpty())
                .mapToPair(month -> new Tuple2<>(month, 1))
                .reduceByKey((a, b) -> a + b);

        monthlyCounts.foreachRDD(rdd -> {
            rdd.foreach(tuple -> {
                String month = tuple._1(); // ex: "2023-05"
                Integer count = tuple._2(); // ex: 1

                // Transformer "2023-05" → "2023/05" pour éviter confusion avec split()
                String formattedMonth = month.replace('-', '/');

                // Construire le message Kafka au format attendu par Python
                String message = formattedMonth + "-" + count; // ex: "2023/05-1"

                System.out.println("Mois : " + formattedMonth + ", Count : " + count);

                // Envoi vers Kafka
                KafkaResultProducer.sendResult("monthly_trend", formattedMonth, message);
            });

            if (rdd.getStorageLevel().useMemory()) {
                rdd.unpersist();
            }
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
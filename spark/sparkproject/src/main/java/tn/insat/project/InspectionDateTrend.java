package tn.insat.project;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import scala.Tuple2;

import java.util.*;

public class InspectionDateTrend {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("InspectionDateTrend").setMaster("local[*]").set("spark.streaming.blockSize", "64m");;
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Paramètres Kafka
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "inspection-date-group");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("inspections");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaDStream<String> lines = stream.map(record -> record.value());

        JavaDStream<String> inspectionDates = lines.map(line -> {
            String[] parts = line.split(",");
            if (parts.length > 12 && !parts[12].isEmpty()) {
                return parts[12]; // inspection_datea
            }
            return "";
        });

        JavaPairDStream<String, Integer> dailyInspections = inspectionDates
                .filter(date -> !date.isEmpty())
                .mapToPair(date -> new Tuple2<>(date, 1))
                .reduceByKey((a, b) -> a + b);

        dailyInspections.foreachRDD(rdd -> {
            rdd.foreach(tuple -> {
                String date = tuple._1(); // ex: "2023-12-15"
                Integer count = tuple._2(); // ex: 1

                // Transformer la date pour éviter confusion avec split('-')
                String formattedDate = date.replace('-', '/'); // "2023/12/15"

                // Construire le message Kafka au format attendu
                String message = formattedDate + "-" + count; // "2023/12/15-1"

                System.out.println("Date : " + formattedDate + ", Count : " + count);
                KafkaResultProducer.sendResult("daily_inspections", formattedDate, message);
            });
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
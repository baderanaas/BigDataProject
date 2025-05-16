package tn.insat.project;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class ResultDateAnalysis {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf()
                .setAppName("InspectionDateTrend")
                .setMaster("local[*]")
                .set("spark.streaming.blockSize", "64m")
                .set("spark.ui.port", "4050");
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

        JavaDStream<String> dates = lines.map(line -> {
            String[] parts = line.split(",");

            if (parts.length > 14) {
                String resultDate = parts[14]; // ex: "2021-01-12"

                if (Pattern.matches("\\d{4}-\\d{2}-\\d{2}", resultDate)) {
                    return resultDate;
                }
            }
            return "";
        });

        JavaPairDStream<String, Integer> dailyCount = dates
                .filter(date -> !date.isEmpty())
                .mapToPair(date -> new Tuple2<>(date, 1))
                .reduceByKey((a, b) -> a + b);

        dailyCount.foreachRDD(rdd -> {
            rdd.foreach(tuple -> {
                System.out.println("Date de résultat : " + tuple._1() + ", Count : " + tuple._2());
                String key = tuple._1(); // ex: "2023-12-15"
                String value = tuple._1().replace("-", "/") + "-" + tuple._2(); // ex: "2023/12/15-1"
                KafkaResultProducer.sendResult("results_by_date", key, value);
            });
            if (rdd.getStorageLevel().useMemory()) {
                rdd.unpersist();}
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
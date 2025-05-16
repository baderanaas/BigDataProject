import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaInspectionConsumer {
    public static void main(String[] args) throws InterruptedException {

        SparkConf conf = new SparkConf().setAppName("KafkaInspectionConsumer").setMaster("local[*]");
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

        // Créer un flux à partir de Kafka
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        // Extraire la valeur (CSV)
        JavaDStream<String> lines = stream.map(record -> record.value());

        // Afficher les lignes reçues
        lines.foreachRDD(rdd -> {
            rdd.foreach(line -> System.out.println("Reçu : " + line));
        });

        jssc.start();
        jssc.awaitTermination();
    }
}
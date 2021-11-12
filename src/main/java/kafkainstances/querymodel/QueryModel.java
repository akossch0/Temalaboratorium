package kafkainstances.querymodel;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import kafkainstances.commandmodel.CommandModel;
import org.apache.kafka.clients.consumer.*;
import recordmodels.ExamApplicationRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class QueryModel{
    public static void main(final String[] args) {
        Properties props = null;
        try {
            props = CommandModel.loadConfig("C:\\Users\\koss6\\IntelliJIDEAProjects\\java.cfg");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Add additional properties.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        //props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, DataRecord.class);
        props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, ExamApplicationRecord.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        final Consumer<Integer, ExamApplicationRecord> consumer = new KafkaConsumer<Integer, ExamApplicationRecord>(props);
        consumer.subscribe(Arrays.asList("NewTopic"));

        try {
            while (true) {
                ConsumerRecords<Integer, ExamApplicationRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Integer, ExamApplicationRecord> record : records) {
                    Integer key = record.key();
                    ExamApplicationRecord value = record.value();
                    //total_count += value.getCount();
                    ///Long delay = System.nanoTime()-value;
                    //double delayInMs = (double) delay/1000000;
                    System.out.println("Consumed record with key " + key + " and value of " + value.toString());
                }
            }
        } finally {
            consumer.close();
        }
    }
}


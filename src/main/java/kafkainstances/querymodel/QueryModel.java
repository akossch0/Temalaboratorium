package kafkainstances.querymodel;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import recordmodels.ExamApplicationRecord;
import recordmodels.Subject;
import students.Student;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

import static main.Main.loadConfig;

public class QueryModel{

    static Queue<ExamApplicationRecord> applicationRecords = new ArrayBlockingQueue<>(100);
    static Queue<Float> delays = new ArrayBlockingQueue<>(100);

    public static void main(final String[] args) {

        Runnable consumerThread = () -> {
            Properties props = null;
            try {
                props = loadConfig("C:\\Users\\koss6\\IntelliJIDEAProjects\\java.cfg");
            } catch (IOException e) {
                e.printStackTrace();
            }

            // Add additional properties.
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer");
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
            props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, ExamApplicationRecord.class);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");


            final Consumer<Long, ExamApplicationRecord> consumer = new KafkaConsumer<Long, ExamApplicationRecord>(props);
            consumer.subscribe(Arrays.asList("NewTopic"));

            try {
                while (true) {
                    ConsumerRecords<Long, ExamApplicationRecord> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<Long, ExamApplicationRecord> record : records) {
                        Long key = record.key();
                        ExamApplicationRecord value = record.value();
                        //applicationRecords.add(value);
                        //total_count += value.getCount();
                        long directDelay = System.nanoTime() - key;
                        long recordDelay = System.nanoTime() - value.getTimestamp();
                        float directDelayInMs = (float) directDelay/1000000;
                        float recordDelayInMs = (float) recordDelay/1000000;
                        //delays.add(directDelayInMs);
                        System.out.println("Consumed a record with " + directDelayInMs + " ms direct delay and " + recordDelayInMs + "ms record delay.");
                        //System.out.println("Consumed record with value of " + value.toString() + " and delay of " + directDelayInMs + " ms");

                    }
                }
            } finally {
                consumer.close();
            }
        };

        new Thread(consumerThread,"consumer").start();

    }
}


package kafkainstances.commandmodel;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import recordmodels.ExamApplicationRecord;
import recordmodels.Subject;
import students.IExamApplied;
import students.Student;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;

/**
 * Command model of the CQRS pattern, can understand incoming exam applications, and produce messages based on these applications
 */
public class CommandModel implements IExamApplied, Runnable {

    Queue<ExamApplicationRecord> applicationRecords = new ArrayBlockingQueue<>(100);

    @Override
    public void AppliedForExam(Student student, Subject subject, Date date) {
        ExamApplicationRecord record = new ExamApplicationRecord(
                student.getName(), student.getUserId(), subject, date, System.nanoTime());
        synchronized (applicationRecords) {
            applicationRecords.add(record);
            //System.out.println("Added" + applicationRecords.size());
        }
    }

    public void produce(ExamApplicationRecord record){

    }
    @Override
    public void run() {
        // Load properties from a local configuration file
        // Create the configuration file (e.g. at '$HOME/.confluent/java.config') with configuration parameters
        // to connect to your Kafka cluster, which can be on your local host, Confluent Cloud, or any other cluster.
        // Follow these instructions to create this file: https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/java.html
        //TODO: creating config file and giving filepath
        Properties props = null;
        try {
            props = loadConfig("C:\\Users\\koss6\\IntelliJIDEAProjects\\java.cfg");
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Create topic if needed //TODO: selecting topic name
        final String topic = "NewTopic";
        createTopic(topic, props);

        // Add additional properties.
        //props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");

        Producer<Integer, ExamApplicationRecord> producer = new KafkaProducer<Integer, ExamApplicationRecord>(props);
        // Produce sample data

        Integer i = 0;
        while(true) {
            if (!applicationRecords.isEmpty()) {
                for (ExamApplicationRecord record : applicationRecords) {
                    //System.out.println("Producing record: " + i + "\t" + record.toString());
                    //Thread.sleep(10);
                    producer.send(new ProducerRecord<Integer, ExamApplicationRecord>(topic, i++, record), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                e.printStackTrace();
                            }
                        }
                    });
                    applicationRecords.remove();
                    //System.out.println(applicationRecords.size());
                }
                /*ExamApplicationRecord record = applicationRecords.remove();
                producer.send(new ProducerRecord<Integer, ExamApplicationRecord>(topic, i++, record), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null) {
                            e.printStackTrace();
                        }
                    }
                });*/

            }
        }
        //producer.flush();

        //producer.close();
    }

    // Create topic in Confluent Cloud
    public static void createTopic(final String topic,
                                   final Properties cloudConfig) {
        final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}

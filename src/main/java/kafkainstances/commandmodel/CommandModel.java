package kafkainstances.commandmodel;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.TopicExistsException;
import recordmodels.ExamApplicationRecord;
import recordmodels.Subject;
import students.IExamApplied;
import students.Student;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;

import static main.Main.loadConfig;

/**
 * Command model of the CQRS pattern, can understand incoming exam applications, and produce messages based on these applications
 */
public class CommandModel implements IExamApplied, Runnable {

    private static CommandModel instance = null;
    public static final Object lock = new Object();

    Queue<ExamApplicationRecord> applicationRecords = new ArrayBlockingQueue<>(100);

    public static CommandModel getInstance() {
        if (instance == null)
            instance = new CommandModel();
        return instance;
    }

    private CommandModel(){}

    @Override
    public void AppliedForExam(Student student, Subject subject, Date date) {
        ExamApplicationRecord record = new ExamApplicationRecord(
                student.getName(), student.getUserId(), subject, date, System.nanoTime());

        applicationRecords.add(record);
        System.out.println("Added" + record.getUserId());
    }

    @Override
    public void run() {
        Properties props = null;
        try {
            props = loadConfig("C:\\Users\\koss6\\IntelliJIDEAProjects\\java.cfg");
        } catch (IOException e) {
            e.printStackTrace();
        }

        final String topic = "NewTopic";
        createTopic(topic, props);

        // Add additional properties.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<Long, ExamApplicationRecord> producer = new KafkaProducer<Long, ExamApplicationRecord>(props);

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.initTransactions();

        synchronized (lock) {
            lock.notifyAll();
        }

        TransactionForEachRecord(producer, topic);
        //TransactionForMultipleRecords(producer, topic);
        //ProducingWithoutTransactions(producer, topic);

        producer.flush();
        producer.close();
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

    private void TransactionForMultipleRecords(Producer<Long, ExamApplicationRecord> producer, String topic){
        int i = 0;
        boolean started = false;
        while (true) {
            try {
                if (!applicationRecords.isEmpty()) {
                    started = true;
                    producer.beginTransaction();
                    System.out.println("Starting transaction-----------------------------------------");
                    for (ExamApplicationRecord record : applicationRecords) {
                        ProduceSingleRecord(i++, record, producer, topic);
                        applicationRecords.remove();
                    }
                    producer.commitTransaction();
                    System.out.println("Committing-----------------------------------------");
                }else if(started){
                    break;
                }

            } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                // We can't recover from these exceptions, so our only option is to close the producer and exit.
                producer.close();
            } catch (KafkaException e) {
                // For all other exceptions, just abort the transaction and try again.
                producer.abortTransaction();
            }
        }
    }

    private void TransactionForEachRecord(Producer<Long, ExamApplicationRecord> producer, String topic){
        int i = 0;
        boolean started = false;
        while (true) {
            if (!applicationRecords.isEmpty()) {
                started = true;
                for (ExamApplicationRecord record : applicationRecords) {
                    try {
                        producer.beginTransaction();
                        System.out.println("--Starting transaction--");
                        ProduceSingleRecord(i++, record, producer, topic);
                        applicationRecords.remove();
                        producer.commitTransaction();
                        System.out.println("--Committing transaction--");
                    } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
                        // We can't recover from these exceptions, so our only option is to close the producer and exit.
                        producer.close();
                    } catch (KafkaException e) {
                        // For all other exceptions, just abort the transaction and try again.
                        producer.abortTransaction();
                    }
                }
            }else if(started){
                break;
            }
        }
    }

    private void ProducingWithoutTransactions(Producer<Long, ExamApplicationRecord> producer, String topic){
        int i = 0;
        while (true) {
            if (!applicationRecords.isEmpty()) {
                for (ExamApplicationRecord record : applicationRecords) {
                    ProduceSingleRecord(i++, record, producer, topic);
                    applicationRecords.remove();
                    producer.commitTransaction();
                    System.out.println("Committing transaction");
                }
            }
        }
    }

    private void ProduceSingleRecord(int i, ExamApplicationRecord record, Producer<Long, ExamApplicationRecord> producer, String topic){
        System.out.println("Producing record: " + i++ + ". record\t" + record.toString() + " Size of records: " + applicationRecords.size());
        producer.send(new ProducerRecord<Long, ExamApplicationRecord>(topic, System.nanoTime(), record));
    }


}

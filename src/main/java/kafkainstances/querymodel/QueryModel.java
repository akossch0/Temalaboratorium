package kafkainstances.querymodel;

import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import recordmodels.ExamApplicationRecord;
import recordmodels.Subject;
import students.Student;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;

import static main.Main.loadConfig;

/**
 * Query model of the CQRS pattern, can consume from the single point of truth,
 * the topic in which all the messages (exam applications) are produced.
 * Stores the delay based on the key, and the timestamp of the value.
 */
public class QueryModel{
    static Queue<Float> delaysDirect = new ArrayBlockingQueue<>(100);
    static Queue<Float> delaysFromRecording = new ArrayBlockingQueue<>(100);

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
                    ConsumerRecords<Long, ExamApplicationRecord> records = consumer.poll(Duration.ofMillis(1000));
                    for (ConsumerRecord<Long, ExamApplicationRecord> record : records) {
                        Long key = record.key();
                        ExamApplicationRecord value = record.value();
                        long directDelay = System.nanoTime() - key;
                        long recordDelay = System.nanoTime() - value.getTimestamp();
                        float directDelayInMs = (float) directDelay/1000000;
                        float recordDelayInMs = (float) recordDelay/1000000;
                        delaysDirect.add(directDelayInMs);
                        delaysFromRecording.add(recordDelayInMs);
                        System.out.println("Consumed " + value.getName() + "'s record with " + directDelayInMs + " ms direct delay and " + recordDelayInMs + " ms record delay.");
                    }
                }
            } finally {
                consumer.close();
            }
        };

        new Thread(consumerThread,"consumer").start();

        Runnable fileOut = () -> {
            FileWriter myWriter = null;
            FileWriter myWriter2 = null;
            try {

            while(true){
                if(delaysDirect.size() == 100 && delaysFromRecording.size() == 100){
                    Scanner myReader = new Scanner(new File(
                            "C:\\Users\\koss6\\OneDrive\\Asztali gép\\University\\BME\\5\\Témalaboratórium\\project\\java\\src\\output.txt"));
                    Scanner myReader2 = new Scanner(new File(
                            "C:\\Users\\koss6\\OneDrive\\Asztali gép\\University\\BME\\5\\Témalaboratórium\\project\\java\\src\\output2.txt"));
                    int numberOfTests = Integer.parseInt(myReader.nextLine());
                    myReader2.nextLine();
                    int j = 0,k = 0;
                    List<Float> dd = new ArrayList<>(delaysDirect);
                    List<Float> dr = new ArrayList<>(delaysFromRecording);
                    while (myReader.hasNextLine()) {
                        String data = myReader.nextLine();
                        Float newAverage = (numberOfTests * Float.parseFloat(data) + dd.get(j)) / (numberOfTests+1);
                        dd.set(j, newAverage);
                        j++;
                    }
                    while (myReader2.hasNextLine()) {
                        String data = myReader2.nextLine();
                        Float newAverage = (numberOfTests * Float.parseFloat(data) + dr.get(k)) / (numberOfTests+1);
                        dr.set(k, newAverage);
                        k++;
                    }
                    myWriter = new FileWriter(
                            "C:\\Users\\koss6\\OneDrive\\Asztali gép\\University\\BME\\5\\Témalaboratórium\\project\\java\\src\\output.txt");
                    myWriter2 = new FileWriter(
                            "C:\\Users\\koss6\\OneDrive\\Asztali gép\\University\\BME\\5\\Témalaboratórium\\project\\java\\src\\output2.txt");

                    myWriter.write(numberOfTests+1 + "\n");
                    myWriter2.write(numberOfTests+1 + "\n");
                    for(int i = 0; i < 100; i++){
                        myWriter.write(dd.get(i) + "\n");
                        myWriter2.write(dr.get(i) + "\n" );
                    }
                    delaysDirect.clear();
                    delaysFromRecording.clear();
                    myWriter.close();
                    myWriter2.close();
                    myReader.close();
                    myReader2.close();
                    //break;
                }
                Thread.sleep(500);
            }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        };

        new Thread(fileOut, "fileOut").start();
    }
}


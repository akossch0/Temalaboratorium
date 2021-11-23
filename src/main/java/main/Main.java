package main;

import kafkainstances.commandmodel.CommandModel;
import recordmodels.Subject;
import students.Student;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    public static void main(final String[] args) throws InterruptedException {

        CommandModel commandModel = CommandModel.getInstance();
        new Thread(commandModel, "command").start();


        List<Student> students = new ArrayList<>();
        List<Subject> subjects = new ArrayList<>(Arrays.asList(Subject.values()));
        for(int i = 0; i < 100; i++){
            students.add(new Student("Jancsi" + String.valueOf(i), String.valueOf(i), subjects, CommandModel.getInstance()));
        }

        synchronized (CommandModel.lock){
            CommandModel.lock.wait();
        }
        Runnable applicationGenerator = () -> {
            for (Student s : students) {
                //try {
                //    Thread.sleep(3);
                //} catch (InterruptedException e) {
                //    e.printStackTrace();
                //}
                s.ApplyForExam(Subject.TERMINFO, new Date(System.currentTimeMillis()));
            }
        };
        new Thread(applicationGenerator, "simulate").start();

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

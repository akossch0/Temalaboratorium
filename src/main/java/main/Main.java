package main;

import kafkainstances.commandmodel.CommandModel;
import recordmodels.Subject;
import students.Student;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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
                try {
                    Thread.sleep(3);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                s.ApplyForExam(Subject.TERMINFO, new Date(System.currentTimeMillis()));
            }
        };
        new Thread(applicationGenerator, "simulate").start();

    }
}

package main;

import kafkainstances.commandmodel.CommandModel;
import kafkainstances.querymodel.QueryModel;
import recordmodels.Subject;
import students.Student;

import java.io.IOException;
import java.util.*;

public class Main {
    public static void main(final String[] args) throws InterruptedException {
        CommandModel commandModel = new CommandModel();
        List<Student> students = new ArrayList<>();
        List<Subject> subjects = new ArrayList<>();
        for(Subject s: Subject.values()){
            subjects.add(s);
        }
        for(int i = 0; i < 100; i++){
            students.add(new Student("Jancsi" + String.valueOf(i), String.valueOf(i),subjects ,commandModel));
        }
        //QueryModel queryModel = new QueryModel();
        //new Thread(queryModel, "query").start();

        new Thread(commandModel, "command").start();

        Runnable applicationSimulator = () -> {
            for (Student s : students) {
                s.ApplyForExam(Subject.TERMINFO, new Date(System.currentTimeMillis()));
            }
        };
        new Thread(applicationSimulator, "simulate").start();


    }
}

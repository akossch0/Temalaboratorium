package students;

import recordmodels.Subject;

import java.util.*;

/**
 * Representation of a student
 */
public class Student {

    private String name;
    private String userId;
    private List<Subject> subjects;
    private IExamApplied examApplication;

    /**
     * Constructor of Student class
     * @param name              Name of the student
     * @param userId            i.e. Neptun-id
     * @param subjects          List of enrolled subjects
     * @param examApplication   A student can apply for an exam through this
     */
    public Student(String name, String userId, List<Subject> subjects, IExamApplied examApplication) {
        this.name = name;
        this.userId = userId;
        this.subjects = subjects;
        this.examApplication = examApplication;
    }


    public void setExamApplication(IExamApplied examApplication) {
        this.examApplication = examApplication;
    }

    public String getName() {
        return name;
    }

    public String getUserId() {
        return userId;
    }

    public List<Subject> getSubjects() {
        return subjects;
    }

    public IExamApplied getExamApplication() {
        return examApplication;
    }

    public void ApplyForExam(Subject subject, Date date){
        examApplication.AppliedForExam(this, subject, date);
    }
}

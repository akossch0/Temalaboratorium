package students;

import recordmodels.Subject;

import java.util.Date;

/**
 * Interface that the Command model has to implement (to receive applications from students)
 */
public interface IExamApplied {
    void AppliedForExam(Student student, Subject subject, Date date);
}

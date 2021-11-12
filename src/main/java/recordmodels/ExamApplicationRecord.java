package recordmodels;

import java.util.Date;

/**
 * A record that is produced, when a student applies for an exam
 */
public class ExamApplicationRecord {
    private String name;
    private String userId;
    private Subject subject;
    private Date examDate;
    private Long timestamp;

    public ExamApplicationRecord() {}

    /**
     * Constructor of the record class with all parameters
     * @param name      Name of the student
     * @param userId    i.e. Neptun-id
     * @param subject   The subject of the exam
     * @param examDate  The date of the exam
     * @param timestamp Timestamp of the record sent
     */
    public ExamApplicationRecord(String name, String userId, Subject subject, Date examDate, Long timestamp) {
        this.name = name;
        this.userId = userId;
        this.subject = subject;
        this.examDate = examDate;
        this.timestamp = timestamp;
    }

    public String getName() {
        return name;
    }

    public String getUserId() {
        return userId;
    }

    public Subject getSubject() {
        return subject;
    }

    public Date getExamDate() {
        return examDate;
    }

    public Long getTimestamp() {
        return timestamp;
    }
    public void setName(String name) {
        this.name = name;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    public void setExamDate(Date examDate) {
        this.examDate = examDate;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String toString() {
        return new com.google.gson.Gson().toJson(this);
    }

}

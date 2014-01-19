package net.steveperkins.fitnessjiffy.etl.model;

import java.util.Date;
import java.util.UUID;

public class ExercisePerformed {

    private UUID id;
    private UUID exerciseId;
    private Date date;
    private Integer minutes;

    public ExercisePerformed(UUID id, UUID exerciseId, Date date, Integer minutes) {
        this.id = id;
        this.exerciseId = exerciseId;
        this.date = date;
        this.minutes = minutes;
    }

    public ExercisePerformed() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getExerciseId() {
        return exerciseId;
    }

    public void setExerciseId(UUID exerciseId) {
        this.exerciseId = exerciseId;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Integer getMinutes() {
        return minutes;
    }

    public void setMinutes(Integer minutes) {
        this.minutes = minutes;
    }

}

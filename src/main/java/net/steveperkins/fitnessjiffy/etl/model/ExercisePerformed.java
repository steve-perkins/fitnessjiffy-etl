package net.steveperkins.fitnessjiffy.etl.model;

import javax.annotation.Nonnull;
import java.sql.Date;
import java.util.UUID;

public class ExercisePerformed {

    private UUID id;
    private UUID exerciseId;
    private Date date;
    private Integer minutes;

    public ExercisePerformed(
            @Nonnull UUID id,
            @Nonnull UUID exerciseId,
            @Nonnull Date date,
            @Nonnull Integer minutes
    ) {
        this.id = new UUID(id.getMostSignificantBits(), id.getLeastSignificantBits());
        this.exerciseId = new UUID(exerciseId.getMostSignificantBits(), exerciseId.getLeastSignificantBits());
        this.date = new Date(date.getTime());
        this.minutes = minutes;
    }

    public ExercisePerformed() {
    }

    @Nonnull
    public UUID getId() {
        return id;
    }

    public void setId(@Nonnull UUID id) {
        this.id = id;
    }

    @Nonnull
    public UUID getExerciseId() {
        return exerciseId;
    }

    public void setExerciseId(@Nonnull UUID exerciseId) {
        this.exerciseId = new UUID(exerciseId.getMostSignificantBits(), exerciseId.getLeastSignificantBits());
    }

    @Nonnull
    public Date getDate() {
        return (Date) date.clone();
    }

    public void setDate(@Nonnull Date date) {
        this.date = new Date(date.getTime());
    }

    @Nonnull
    public Integer getMinutes() {
        return minutes;
    }

    public void setMinutes(@Nonnull Integer minutes) {
        this.minutes = minutes;
    }

}

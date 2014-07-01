package net.steveperkins.fitnessjiffy.etl.model;

import javax.annotation.Nonnull;
import java.sql.Date;
import java.util.UUID;

public class Weight {

    private UUID id;
    private Date date;
    private Double pounds;

    public Weight(
            @Nonnull UUID id,
            @Nonnull Date date,
            @Nonnull Double pounds
    ) {
        this.id = id;
        this.date = (Date) date.clone();
        this.pounds = pounds;
    }

    public Weight() {
    }

    @Nonnull
    public UUID getId() {
        return id;
    }

    public void setId(@Nonnull UUID id) {
        this.id = id;
    }

    @Nonnull
    public Date getDate() {
        return (Date) date.clone();
    }

    public void setDate(@Nonnull Date date) {
        this.date = (Date) date.clone();
    }

    @Nonnull
    public Double getPounds() {
        return pounds;
    }

    public void setPounds(@Nonnull Double pounds) {
        this.pounds = pounds;
    }

}

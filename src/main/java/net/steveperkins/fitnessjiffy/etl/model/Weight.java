package net.steveperkins.fitnessjiffy.etl.model;

import javax.annotation.Nonnull;
import java.sql.Date;
import java.util.UUID;

public final class Weight {

    private UUID id;
    private Date date;
    private Double pounds;

    public Weight(
            @Nonnull final UUID id,
            @Nonnull final Date date,
            @Nonnull final Double pounds
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

    public void setId(@Nonnull final UUID id) {
        this.id = id;
    }

    @Nonnull
    public Date getDate() {
        return (Date) date.clone();
    }

    public void setDate(@Nonnull final Date date) {
        this.date = (Date) date.clone();
    }

    @Nonnull
    public Double getPounds() {
        return pounds;
    }

    public void setPounds(@Nonnull final Double pounds) {
        this.pounds = pounds;
    }

}

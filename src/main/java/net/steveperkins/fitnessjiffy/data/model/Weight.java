package net.steveperkins.fitnessjiffy.data.model;

import java.util.Date;
import java.util.UUID;

public class Weight {

    private UUID id;
    private Date date;
    private Double pounds;

    public Weight(UUID id, Date date, Double pounds) {
        this.id = id;
        this.date = date;
        this.pounds = pounds;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Double getPounds() {
        return pounds;
    }

    public void setPounds(Double pounds) {
        this.pounds = pounds;
    }

}

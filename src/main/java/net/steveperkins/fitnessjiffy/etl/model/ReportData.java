package net.steveperkins.fitnessjiffy.etl.model;

import javax.annotation.Nonnull;
import java.sql.Date;
import java.util.UUID;

public final class ReportData {

    private UUID id;
    private Date date;
    private Double pounds;
    private Integer netCalories;
    private Double netPoints;

    public ReportData(
            @Nonnull final UUID id,
            @Nonnull final Date date,
            @Nonnull final Double pounds,
            @Nonnull final Integer netCalories,
            @Nonnull final Double netPoints
    ) {
        this.id = id;
        this.date = (Date) date.clone();
        this.pounds = pounds;
        this.netCalories = netCalories;
        this.netPoints = netPoints;
    }

    public ReportData() {
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

    @Nonnull
    public Integer getNetCalories() {
        return netCalories;
    }

    public void setNetCalories(@Nonnull final Integer netCalories) {
        this.netCalories = netCalories;
    }

    @Nonnull
    public Double getNetPoints() {
        return netPoints;
    }

    public void setNetPoints(@Nonnull final Double netPoints) {
        this.netPoints = netPoints;
    }
}

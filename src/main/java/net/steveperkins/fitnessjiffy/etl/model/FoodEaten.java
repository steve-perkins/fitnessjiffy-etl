package net.steveperkins.fitnessjiffy.etl.model;

import javax.annotation.Nonnull;
import java.sql.Date;
import java.util.UUID;

public final class FoodEaten {

    private UUID id;
    private UUID foodId;
    private Date date;
    private Food.ServingType servingType;
    private Double servingQty;

    public FoodEaten(
            @Nonnull final UUID id,
            @Nonnull final UUID foodId,
            @Nonnull final Date date,
            @Nonnull final Food.ServingType servingType,
            @Nonnull final Double servingQty
    ) {
        this.id = id;
        this.foodId = foodId;
        this.date = (Date) date.clone();
        this.servingType = servingType;
        this.servingQty = servingQty;
    }

    public FoodEaten() {
    }

    @Nonnull
    public UUID getId() {
        return id;
    }

    public void setId(@Nonnull final UUID id) {
        this.id = id;
    }

    @Nonnull
    public UUID getFoodId() {
        return foodId;
    }

    public void setFoodId(@Nonnull final UUID foodId) {
        this.foodId = foodId;
    }

    @Nonnull
    public Date getDate() {
        return (Date) date.clone();
    }

    public void setDate(@Nonnull final Date date) {
        this.date = (Date) date.clone();
    }

    @Nonnull
    public Food.ServingType getServingType() {
        return servingType;
    }

    public void setServingType(@Nonnull final Food.ServingType servingType) {
        this.servingType = servingType;
    }

    @Nonnull
    public Double getServingQty() {
        return servingQty;
    }

    public void setServingQty(@Nonnull final Double servingQty) {
        this.servingQty = servingQty;
    }

}

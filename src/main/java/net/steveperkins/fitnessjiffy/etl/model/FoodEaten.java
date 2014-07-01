package net.steveperkins.fitnessjiffy.etl.model;

import javax.annotation.Nonnull;
import java.sql.Date;
import java.util.UUID;

public class FoodEaten {

    private UUID id;
    private UUID foodId;
    private Date date;
    private Food.ServingType servingType;
    private Double servingQty;

    public FoodEaten(
            @Nonnull UUID id,
            @Nonnull UUID foodId,
            @Nonnull Date date,
            @Nonnull Food.ServingType servingType,
            @Nonnull Double servingQty
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

    public void setId(@Nonnull UUID id) {
        this.id = id;
    }

    @Nonnull
    public UUID getFoodId() {
        return foodId;
    }

    public void setFoodId(@Nonnull UUID foodId) {
        this.foodId = foodId;
    }

    @Nonnull
    public Date getDate() {
        return (Date) date.clone();
    }

    public void setDate(@Nonnull Date date) {
        this.date = (Date) date.clone();
    }

    @Nonnull
    public Food.ServingType getServingType() {
        return servingType;
    }

    public void setServingType(@Nonnull Food.ServingType servingType) {
        this.servingType = servingType;
    }

    @Nonnull
    public Double getServingQty() {
        return servingQty;
    }

    public void setServingQty(@Nonnull Double servingQty) {
        this.servingQty = servingQty;
    }

}

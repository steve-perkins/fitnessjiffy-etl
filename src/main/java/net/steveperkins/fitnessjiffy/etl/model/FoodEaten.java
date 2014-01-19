package net.steveperkins.fitnessjiffy.etl.model;

import java.util.Date;
import java.util.UUID;

public class FoodEaten {

    private UUID id;
    private UUID foodId;
    private Date date;
    private Food.ServingType servingType;
    private Double servingQty;

    public FoodEaten(UUID id, UUID foodId, Date date, Food.ServingType servingType, Double servingQty) {
        this.id = id;
        this.foodId = foodId;
        this.date = date;
        this.servingType = servingType;
        this.servingQty = servingQty;
    }

    public FoodEaten() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getFoodId() {
        return foodId;
    }

    public void setFoodId(UUID foodId) {
        this.foodId = foodId;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public Food.ServingType getServingType() {
        return servingType;
    }

    public void setServingType(Food.ServingType servingType) {
        this.servingType = servingType;
    }

    public Double getServingQty() {
        return servingQty;
    }

    public void setServingQty(Double servingQty) {
        this.servingQty = servingQty;
    }

}

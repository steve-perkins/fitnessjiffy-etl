package net.steveperkins.fitnessjiffy.etl.model;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.UUID;

public class Food {

    public enum ServingType {

        OUNCE(1), CUP(8), POUND(16), PINT(16), TABLESPOON(0.5), TEASPOON(0.1667), GRAM(0.03527), CUSTOM(0);

        private double value;

        private ServingType(double value) {
            this.value = value;
        }

        @Nullable
        public static ServingType fromValue(double value) {
            for(ServingType servingType : ServingType.values()) {
                if(servingType.getValue() == value) {
                    return servingType;
                }
            }
            return null;
        }

        @Nullable
        public static ServingType fromString(@Nonnull String s) {
            for(ServingType servingType : ServingType.values()) {
                if(servingType.toString().equalsIgnoreCase(s)) {
                    return servingType;
                }
            }
            return null;
        }

        public double getValue() {
            return this.value;
        }

    }

    private UUID id;
    private String name;
    private ServingType defaultServingType;
    private Double servingTypeQty;
    private Integer calories;
    private Double fat;
    private Double saturatedFat;
    private Double carbs;
    private Double fiber;
    private Double sugar;
    private Double protein;
    private Double sodium;
    private Timestamp createdTime;
    private Timestamp lastUpdatedTime;

    public Food(
            @Nonnull UUID id,
            @Nonnull String name,
            @Nonnull ServingType defaultServingType,
            @Nonnull Double servingTypeQty,
            @Nonnull Integer calories,
            @Nonnull Double fat,
            @Nonnull Double saturatedFat,
            @Nonnull Double carbs,
            @Nonnull Double fiber,
            @Nonnull Double sugar,
            @Nonnull Double protein,
            @Nonnull Double sodium,
            @Nonnull Timestamp createdTime,
            @Nonnull Timestamp lastUpdatedTime
    ) {
        this.id = id;
        this.name = name;
        this.defaultServingType = defaultServingType;
        this.servingTypeQty = servingTypeQty;
        this.calories = calories;
        this.fat = fat;
        this.saturatedFat = saturatedFat;
        this.carbs = carbs;
        this.fiber = fiber;
        this.sugar = sugar;
        this.protein = protein;
        this.sodium = sodium;
        this.createdTime = (Timestamp) createdTime.clone();
        this.lastUpdatedTime = (Timestamp) lastUpdatedTime.clone();
    }

    public Food() {
    }

    @Nonnull
    public UUID getId() {
        return id;
    }

    public void setId(@Nonnull UUID id) {
        this.id = id;
    }

    @Nonnull
    public String getName() {
        return name;
    }

    public void setName(@Nonnull String name) {
        this.name = name;
    }

    @Nonnull
    public ServingType getDefaultServingType() {
        return defaultServingType;
    }

    public void setDefaultServingType(@Nonnull ServingType defaultServingType) {
        this.defaultServingType = defaultServingType;
    }

    @Nonnull
    public Double getServingTypeQty() {
        return servingTypeQty;
    }

    public void setServingTypeQty(@Nonnull Double servingTypeQty) {
        this.servingTypeQty = servingTypeQty;
    }

    @Nonnull
    public Integer getCalories() {
        return calories;
    }

    public void setCalories(@Nonnull Integer calories) {
        this.calories = calories;
    }

    @Nonnull
    public Double getFat() {
        return fat;
    }

    public void setFat(@Nonnull Double fat) {
        this.fat = fat;
    }

    @Nonnull
    public Double getSaturatedFat() {
        return saturatedFat;
    }

    public void setSaturatedFat(@Nonnull Double saturatedFat) {
        this.saturatedFat = saturatedFat;
    }

    @Nonnull
    public Double getCarbs() {
        return carbs;
    }

    public void setCarbs(@Nonnull Double carbs) {
        this.carbs = carbs;
    }

    @Nonnull
    public Double getFiber() {
        return fiber;
    }

    public void setFiber(@Nonnull Double fiber) {
        this.fiber = fiber;
    }

    @Nonnull
    public Double getSugar() {
        return sugar;
    }

    public void setSugar(@Nonnull Double sugar) {
        this.sugar = sugar;
    }

    @Nonnull
    public Double getProtein() {
        return protein;
    }

    public void setProtein(@Nonnull Double protein) {
        this.protein = protein;
    }

    @Nonnull
    public Double getSodium() {
        return sodium;
    }

    public void setSodium(@Nonnull Double sodium) {
        this.sodium = sodium;
    }

    @Nonnull
    public Timestamp getCreatedTime() {
        return (Timestamp) createdTime.clone();
    }

    public void setCreatedTime(@Nonnull Timestamp createdTime) {
        this.createdTime = (Timestamp) createdTime.clone();
    }

    @Nonnull
    public Timestamp getLastUpdatedTime() {
        return (Timestamp) lastUpdatedTime.clone();
    }

    public void setLastUpdatedTime(@Nonnull Timestamp lastUpdatedTime) {
        this.lastUpdatedTime = (Timestamp) lastUpdatedTime.clone();
    }

}

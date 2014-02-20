package net.steveperkins.fitnessjiffy.etl.model;

import java.util.UUID;

public class Food {

    public enum ServingType {
        OUNCE(1), CUP(8), POUND(16), PINT(16), TABLESPOON(0.5), TEASPOON(0.1667), GRAM(0.03527), CUSTOM(0);
        private double value;
        private ServingType(double value) {
            this.value = value;
        }
        public static ServingType fromValue(double value) {
            for(ServingType servingType : ServingType.values()) {
                if(servingType.getValue() == value) {
                    return servingType;
                }
            }
            return null;
        }
        public static ServingType fromString(String s) {
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

    public Food(UUID id, String name, ServingType defaultServingType, Double servingTypeQty,
                Integer calories, Double fat, Double saturatedFat, Double carbs,
                Double fiber, Double sugar, Double protein, Double sodium) {
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
    }

    public Food() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ServingType getDefaultServingType() {
        return defaultServingType;
    }

    public void setDefaultServingType(ServingType defaultServingType) {
        this.defaultServingType = defaultServingType;
    }

    public Double getServingTypeQty() {
        return servingTypeQty;
    }

    public void setServingTypeQty(Double servingTypeQty) {
        this.servingTypeQty = servingTypeQty;
    }

    public Integer getCalories() {
        return calories;
    }

    public void setCalories(Integer calories) {
        this.calories = calories;
    }

    public Double getFat() {
        return fat;
    }

    public void setFat(Double fat) {
        this.fat = fat;
    }

    public Double getSaturatedFat() {
        return saturatedFat;
    }

    public void setSaturatedFat(Double saturatedFat) {
        this.saturatedFat = saturatedFat;
    }

    public Double getCarbs() {
        return carbs;
    }

    public void setCarbs(Double carbs) {
        this.carbs = carbs;
    }

    public Double getFiber() {
        return fiber;
    }

    public void setFiber(Double fiber) {
        this.fiber = fiber;
    }

    public Double getSugar() {
        return sugar;
    }

    public void setSugar(Double sugar) {
        this.sugar = sugar;
    }

    public Double getProtein() {
        return protein;
    }

    public void setProtein(Double protein) {
        this.protein = protein;
    }

    public Double getSodium() {
        return sodium;
    }

    public void setSodium(Double sodium) {
        this.sodium = sodium;
    }

}

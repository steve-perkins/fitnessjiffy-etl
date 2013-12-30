package net.steveperkins.fitnessjiffy.data.model;

import java.util.UUID;

public class Exercise {

    private UUID id;
    private String code;
    private Double metabolicEquivalent;
    private String category;
    private String description;

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Double getMetabolicEquivalent() {
        return metabolicEquivalent;
    }

    public void setMetabolicEquivalent(Double metabolicEquivalent) {
        this.metabolicEquivalent = metabolicEquivalent;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getDescription() {
        return (description != null) ? description.trim() : null;
    }

    public void setDescription(String description) {
        if(description != null) description = description.trim();
        this.description = description;
    }

    @Override
    public boolean equals(Object other) {
        if(other == null || !(other instanceof Exercise)) return false;
        Exercise that = (Exercise) other;
        return this.id.equals(that.id) && this.code.equals(that.code)
                && this.metabolicEquivalent.equals(that.metabolicEquivalent) && this.category.equals(that.category)
                && this.description.equals(that.description);
    }

}

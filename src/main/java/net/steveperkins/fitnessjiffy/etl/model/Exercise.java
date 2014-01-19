package net.steveperkins.fitnessjiffy.etl.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class Exercise {

    private UUID id;
    private String code;
    private Double metabolicEquivalent;
    private String category;
    private String description;

    @JsonCreator
    public Exercise(@JsonProperty("id") UUID id,
                    @JsonProperty("code") String code,
                    @JsonProperty("metabolicEquivalent") Double metabolicEquivalent,
                    @JsonProperty("category") String category,
                    @JsonProperty("description") String description) {
        this.id = id;
        this.code = code;
        this.metabolicEquivalent = metabolicEquivalent;
        this.category = category;
        this.description = description;
    }

    public Exercise() {
    }

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

}

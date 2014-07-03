package net.steveperkins.fitnessjiffy.etl.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import java.util.UUID;

public final class Exercise {

    private UUID id;
    private String code;
    private Double metabolicEquivalent;
    private String category;
    private String description;

    @JsonCreator
    public Exercise(
            @Nonnull @JsonProperty("id") final UUID id,
            @Nonnull @JsonProperty("code") final String code,
            @Nonnull @JsonProperty("metabolicEquivalent") final Double metabolicEquivalent,
            @Nonnull @JsonProperty("category") final String category,
            @Nonnull @JsonProperty("description") final String description
    ) {
        this.id = id;
        this.code = code;
        this.metabolicEquivalent = metabolicEquivalent;
        this.category = category;
        this.description = description;
    }

    public Exercise() {
    }

    @Nonnull
    public UUID getId() {
        return id;
    }

    public void setId(@Nonnull final UUID id) {
        this.id = id;
    }

    @Nonnull
    public String getCode() {
        return code;
    }

    public void setCode(@Nonnull final String code) {
        this.code = code;
    }

    @Nonnull
    public Double getMetabolicEquivalent() {
        return metabolicEquivalent;
    }

    public void setMetabolicEquivalent(@Nonnull final Double metabolicEquivalent) {
        this.metabolicEquivalent = metabolicEquivalent;
    }

    @Nonnull
    public String getCategory() {
        return category;
    }

    public void setCategory(@Nonnull final String category) {
        this.category = category;
    }

    @Nonnull
    public String getDescription() {
        return description.trim();
    }

    public void setDescription(@Nonnull final String description) {
        this.description = (description == null) ? description : description.trim();
    }

}

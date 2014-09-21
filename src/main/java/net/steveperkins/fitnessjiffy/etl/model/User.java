package net.steveperkins.fitnessjiffy.etl.model;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public final class User {

    public enum Gender {

        MALE, FEMALE;

        @Nullable
        public static Gender fromString(@Nonnull final String s) {
            for (final Gender gender : Gender.values()) {
                if (gender.toString().equalsIgnoreCase(s)) {
                    return gender;
                }
            }
            return null;
        }

        @Override
        @Nonnull
        public String toString() {
            return super.toString();
        }
    }

    public enum ActivityLevel {

        SEDENTARY(1.25), LIGHTLY_ACTIVE(1.3), MODERATELY_ACTIVE(1.5), VERY_ACTIVE(1.7), EXTREMELY_ACTIVE(2.0);

        private double value;

        private ActivityLevel(final double value) {
            this.value = value;
        }

        @Nullable
        public static ActivityLevel fromValue(final double value) {
            for (final ActivityLevel activityLevel : ActivityLevel.values()) {
                if (activityLevel.getValue() == value) {
                    return activityLevel;
                }
            }
            return null;
        }

        @Nullable
        public static ActivityLevel fromString(@Nonnull final String s) {
            for (final ActivityLevel activityLevel : ActivityLevel.values()) {
                if (activityLevel.toString().equalsIgnoreCase(s)) {
                    return activityLevel;
                }
            }
            return null;
        }

        public double getValue() {
            return this.value;
        }

        @Override
        @Nonnull
        public String toString() {
            final StringBuilder s = new StringBuilder(super.toString().toLowerCase().replace('_', ' '));
            for (int index = 0; index < s.length(); index++) {
                if (index == 0 || s.charAt(index - 1) == ' ') {
                    final String currentCharAsString = Character.toString(s.charAt(index));
                    s.replace(index, index + 1, currentCharAsString.toUpperCase());
                }
            }
            return s.toString();
        }

    }

    private UUID id;
    private Gender gender;
    private Date birthdate;
    private double heightInInches;
    private ActivityLevel activityLevel;
    private String email;
    private String passwordHash;
    private String firstName;
    private String lastName;
    private Timestamp createdTime;
    private Timestamp lastUpdatedTime;

    private Set<Weight> weights = new HashSet<>();
    private Set<Food> foods = new HashSet<>();
    private Set<FoodEaten> foodsEaten = new HashSet<>();
    private Set<ExercisePerformed> exercisesPerformed = new HashSet<>();
    private Set<ReportData> reportData = new HashSet<>();

    public User(
            @Nonnull final UUID id,
            @Nonnull final Gender gender,
            @Nonnull final Date birthdate,
            final double heightInInches,
            @Nonnull final ActivityLevel activityLevel,
            @Nonnull final String email,
            @Nonnull final String passwordHash,
            @Nonnull final String firstName,
            @Nonnull final String lastName,
            @Nonnull final Timestamp createdTime,
            @Nonnull final Timestamp lastUpdatedTime,
            @Nonnull final Set<Weight> weights,
            @Nonnull final Set<Food> foods,
            @Nonnull final Set<FoodEaten> foodsEaten,
            @Nonnull final Set<ExercisePerformed> exercisesPerformed
    ) {
        this.id = id;
        this.gender = gender;
        this.birthdate = (Date) birthdate.clone();
        this.heightInInches = heightInInches;
        this.activityLevel = activityLevel;
        this.email = email;
        this.passwordHash = passwordHash;
        this.firstName = firstName;
        this.lastName = lastName;
        this.createdTime = (Timestamp) createdTime.clone();
        this.lastUpdatedTime = (Timestamp) lastUpdatedTime.clone();
        this.weights = weights;
        this.foods = foods;
        this.foodsEaten = foodsEaten;
        this.exercisesPerformed = exercisesPerformed;
    }

    public User() {
    }

    @Nonnull
    public UUID getId() {
        return id;
    }

    public void setId(@Nonnull final UUID id) {
        this.id = id;
    }

    @Nonnull
    public Gender getGender() {
        return gender;
    }

    public void setGender(@Nonnull final Gender gender) {
        this.gender = gender;
    }

    @Nonnull
    public Date getBirthdate() {
        return (Date) birthdate.clone();
    }

    public void setBirthdate(@Nonnull final Date birthdate) {
        this.birthdate = (Date) birthdate.clone();
    }

    public double getHeightInInches() {
        return heightInInches;
    }

    public void setHeightInInches(final double heightInInches) {
        this.heightInInches = heightInInches;
    }

    @Nonnull
    public ActivityLevel getActivityLevel() {
        return activityLevel;
    }

    public void setActivityLevel(@Nonnull final ActivityLevel activityLevel) {
        this.activityLevel = activityLevel;
    }

    @Nonnull
    public String getEmail() {
        return email;
    }

    public void setEmail(@Nonnull final String email) {
        this.email = email;
    }

    @Nonnull
    public String getPasswordHash() {
        return passwordHash;
    }

    public void setPasswordHash(@Nonnull final String passwordHash) {
        this.passwordHash = passwordHash;
    }

    @Nonnull
    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(@Nonnull final String firstName) {
        this.firstName = firstName;
    }

    @Nonnull
    public String getLastName() {
        return lastName;
    }

    public void setLastName(@Nonnull final String lastName) {
        this.lastName = lastName;
    }

    @Nonnull
    public Timestamp getCreatedTime() {
        return (Timestamp) createdTime.clone();
    }

    public void setCreatedTime(@Nonnull final Timestamp createdTime) {
        this.createdTime = (Timestamp) createdTime.clone();
    }

    @Nonnull
    public Timestamp getLastUpdatedTime() {
        return (Timestamp) lastUpdatedTime.clone();
    }

    public void setLastUpdatedTime(@Nonnull final Timestamp lastUpdatedTime) {
        this.lastUpdatedTime = (Timestamp) lastUpdatedTime.clone();
    }

    @Nonnull
    public Set<Weight> getWeights() {
        return weights;
    }

    public void setWeights(@Nonnull final Set<Weight> weights) {
        this.weights = weights;
    }

    @Nonnull
    public Set<Food> getFoods() {
        return foods;
    }

    public void setFoods(@Nonnull final Set<Food> foods) {
        this.foods = foods;
    }

    @Nonnull
    public Set<FoodEaten> getFoodsEaten() {
        return foodsEaten;
    }

    public void setFoodsEaten(@Nonnull final Set<FoodEaten> foodsEaten) {
        this.foodsEaten = foodsEaten;
    }

    @Nonnull
    public Set<ExercisePerformed> getExercisesPerformed() {
        return exercisesPerformed;
    }

    public void setExercisesPerformed(@Nonnull final Set<ExercisePerformed> exercisesPerformed) {
        this.exercisesPerformed = exercisesPerformed;
    }

    @Nonnull
    public Set<ReportData> getReportData() {
        return reportData;
    }

    public void setReportData(@Nonnull final Set<ReportData> reportData) {
        this.reportData = reportData;
    }
}

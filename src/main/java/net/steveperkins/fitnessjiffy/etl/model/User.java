package net.steveperkins.fitnessjiffy.etl.model;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class User {

    public enum Gender {
        MALE, FEMALE;
        public static Gender fromString(String s) {
            for(Gender gender : Gender.values()) {
                if(s != null && gender.toString().equalsIgnoreCase(s)) {
                    return gender;
                }
            }
            return null;
        }
        @Override
        public String toString() {
            return super.toString();
        }
    }

    public enum ActivityLevel {
        SEDENTARY(1.25), LIGHTLY_ACTIVE(1.3), MODERATELY_ACTIVE(1.5), VERY_ACTIVE(1.7), EXTREMELY_ACTIVE(2.0);
        private double value;
        private ActivityLevel(double value) {
            this.value = value;
        }
        public static ActivityLevel fromValue(double value) {
            for(ActivityLevel activityLevel : ActivityLevel.values()) {
                if(activityLevel.getValue() == value) {
                    return activityLevel;
                }
            }
            return null;
        }
        public static ActivityLevel fromString(String s) {
            for(ActivityLevel activityLevel : ActivityLevel.values()) {
                if(activityLevel.toString().equalsIgnoreCase(s)) {
                    return activityLevel;
                }
            }
            return null;
        }
        public double getValue() {
            return this.value;
        }
        @Override
        public String toString() {
            StringBuilder s = new StringBuilder(super.toString().toLowerCase().replace('_', ' '));
            for(int index = 0; index < s.length(); index++) {
                if(index == 0 || s.charAt(index - 1) == ' ') {
                    String currentCharAsString = s.charAt(index) + "";
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
    private byte[] passwordHash;
    private byte[] passwordSalt;
    private String firstName;
    private String lastName;
    private Timestamp createdTime;
    private Timestamp lastUpdatedTime;

    private Set<Weight> weights = new HashSet<>();
    private Set<Food> foods = new HashSet<>();
    private Set<FoodEaten> foodsEaten = new HashSet<>();
    private Set<ExercisePerformed> exercisesPerformed = new HashSet<>();

    public User(
            UUID id,
            Gender gender,
            Date birthdate,
            double heightInInches,
            ActivityLevel activityLevel,
            String email,
            byte[] passwordHash,
            byte[] passwordSalt,
            String firstName,
            String lastName,
            Timestamp createdTime,
            Timestamp lastUpdatedTime,
            Set<Weight> weights,
            Set<Food> foods,
            Set<FoodEaten> foodsEaten,
            Set<ExercisePerformed> exercisesPerformed
    ) {
        this.id = id;
        this.gender = gender;
        this.birthdate = birthdate;
        this.heightInInches = heightInInches;
        this.activityLevel = activityLevel;
        this.email = email;
        this.passwordHash = passwordHash;
        this.passwordSalt = passwordSalt;
        this.firstName = firstName;
        this.lastName = lastName;
        this.createdTime = createdTime;
        this.lastUpdatedTime = lastUpdatedTime;
        this.weights = weights;
        this.foods = foods;
        this.foodsEaten = foodsEaten;
        this.exercisesPerformed = exercisesPerformed;
    }

    public User() {
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public Gender getGender() {
        return gender;
    }

    public void setGender(Gender gender) {
        this.gender = gender;
    }

    public Date getBirthdate() {
        return birthdate;
    }

    public void setBirthdate(Date birthdate) {
        this.birthdate = birthdate;
    }

    public double getHeightInInches() {
        return heightInInches;
    }

    public void setHeightInInches(double heightInInches) {
        this.heightInInches = heightInInches;
    }

    public ActivityLevel getActivityLevel() {
        return activityLevel;
    }

    public void setActivityLevel(ActivityLevel activityLevel) {
        this.activityLevel = activityLevel;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public byte[] getPasswordHash() {
        return passwordHash;
    }

    public void setPasswordHash(byte[] passwordHash) {
        this.passwordHash = passwordHash;
    }

    public byte[] getPasswordSalt() {
        return passwordSalt;
    }

    public void setPasswordSalt(byte[] passwordSalt) {
        this.passwordSalt = passwordSalt;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public Timestamp getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Timestamp createdTime) {
        this.createdTime = createdTime;
    }

    public Timestamp getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    public void setLastUpdatedTime(Timestamp lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public Set<Weight> getWeights() {
        return weights;
    }

    public void setWeights(Set<Weight> weights) {
        this.weights = weights;
    }

    public Set<Food> getFoods() {
        return foods;
    }

    public void setFoods(Set<Food> foods) {
        this.foods = foods;
    }

    public Set<FoodEaten> getFoodsEaten() {
        return foodsEaten;
    }

    public void setFoodsEaten(Set<FoodEaten> foodsEaten) {
        this.foodsEaten = foodsEaten;
    }

    public Set<ExercisePerformed> getExercisesPerformed() {
        return exercisesPerformed;
    }

    public void setExercisesPerformed(Set<ExercisePerformed> exercisesPerformed) {
        this.exercisesPerformed = exercisesPerformed;
    }

}

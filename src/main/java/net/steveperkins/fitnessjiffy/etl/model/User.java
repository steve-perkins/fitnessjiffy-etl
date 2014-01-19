package net.steveperkins.fitnessjiffy.etl.model;

import net.steveperkins.fitnessjiffy.etl.util.NoNullsSet;

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
    private int age;
    private double heightInInches;
    private ActivityLevel activityLevel;
    private String username;
    private String password;
    private String firstName;
    private String lastName;
    private boolean isActive;

    private Set<Weight> weights = new NoNullsSet<>();
    private Set<Food> foods = new NoNullsSet<>();
    private Set<FoodEaten> foodsEaten = new NoNullsSet<>();
    private Set<ExercisePerformed> exercisesPerformed = new NoNullsSet<>();

    public User(UUID id, Gender gender, int age, double heightInInches, ActivityLevel activityLevel, String username,
                String password, String firstName, String lastName, boolean isActive, Set<Weight> weights,
                Set<Food> foods, Set<FoodEaten> foodsEaten, Set<ExercisePerformed> exercisesPerformed) {
        this.id = id;
        this.gender = gender;
        this.age = age;
        this.heightInInches = heightInInches;
        this.activityLevel = activityLevel;
        this.username = username;
        this.password = password;
        this.firstName = firstName;
        this.lastName = lastName;
        this.isActive = isActive;
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

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
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

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
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

    public boolean isActive() {
        return isActive;
    }

    public void setActive(boolean active) {
        isActive = active;
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

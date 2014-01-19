package net.steveperkins.fitnessjiffy.etl.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.steveperkins.fitnessjiffy.etl.util.NoNullsSet;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class Datastore {

    private Set<User> users = new NoNullsSet<>();
    private Set<Food> globalFoods = new NoNullsSet<>();
    private Set<Exercise> exercises = new NoNullsSet<>();

    public Set<User> getUsers() {
        return users;
    }

    public Set<Food> getGlobalFoods() {
        return globalFoods;
    }

    public Set<Exercise> getExercises() {
        return exercises;
    }

    public String toJSONString() throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(this);
    }

    public void toJSONFile(File file) throws IOException {
        new ObjectMapper().writeValue(file, this);
    }

    public static final Datastore fromJSONString(String json) throws IOException {
        return new ObjectMapper().readValue(json, Datastore.class);
    }

    public static final Datastore fromJSONFile(File file) throws IOException {
        return new ObjectMapper().readValue(file, Datastore.class);
    }

}

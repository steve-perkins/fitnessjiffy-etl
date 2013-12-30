package net.steveperkins.fitnessjiffy.data.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.steveperkins.fitnessjiffy.data.util.NoNullsSet;

import java.io.File;
import java.io.IOException;

public class Datastore {

    private NoNullsSet<User> users = new NoNullsSet<>();
    private NoNullsSet<Food> globalFoods = new NoNullsSet<>();
    private NoNullsSet<Exercise> exercises = new NoNullsSet<>();

    public NoNullsSet<User> getUsers() {
        return users;
    }

    public NoNullsSet<Food> getGlobalFoods() {
        return globalFoods;
    }

    public NoNullsSet<Exercise> getExercises() {
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

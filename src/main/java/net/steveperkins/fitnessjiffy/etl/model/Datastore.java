package net.steveperkins.fitnessjiffy.etl.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Datastore {

    private Set<User> users = new HashSet<>();
    private Set<Food> globalFoods = new HashSet<>();
    private Set<Exercise> exercises = new HashSet<>();

    @Nonnull
    public Set<User> getUsers() {
        return users;
    }

    @Nonnull
    public Set<Food> getGlobalFoods() {
        return globalFoods;
    }

    @Nonnull
    public Set<Exercise> getExercises() {
        return exercises;
    }

    @Nonnull
    public String toJSONString() throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(this);
    }

    public void toJSONFile(@Nonnull File file) throws IOException {
        new ObjectMapper().writeValue(file, this);
    }

    public static final Datastore fromJSONString(@Nonnull String json) throws IOException {
        return new ObjectMapper().readValue(json, Datastore.class);
    }

    public static final Datastore fromJSONFile(@Nonnull File file) throws IOException {
        return new ObjectMapper().readValue(file, Datastore.class);
    }

}

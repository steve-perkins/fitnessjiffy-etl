package net.steveperkins.fitnessjiffy.etl.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public final class Datastore {

    private final Set<User> users;
    private final Set<Food> globalFoods;
    private final Set<Exercise> exercises;

    public Datastore() {
        this.users = new HashSet<>();
        this.globalFoods = new HashSet<>();
        this.exercises = new HashSet<>();
    }

    @Nonnull
    public Set<User> getUsers() {
        return Collections.unmodifiableSet(users);
    }

    @Nonnull
    public Set<Food> getGlobalFoods() {
        return Collections.unmodifiableSet(globalFoods);
    }

    @Nonnull
    public Set<Exercise> getExercises() {
        return Collections.unmodifiableSet(exercises);
    }

    public void addUser(@Nonnull final User user) {
        this.users.add(user);
    }

    public void addGlobalFood(@Nonnull final Food globalFood) {
        this.globalFoods.add(globalFood);
    }

    public void addExercise(@Nonnull final Exercise exercise) {
        this.exercises.add(exercise);
    }

    @Nonnull
    public String toJSONString() throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(this);
    }

    public void toJSONFile(@Nonnull final File file) throws IOException {
        new ObjectMapper().writeValue(file, this);
    }

    public static Datastore fromJSONString(@Nonnull final String json) throws IOException {
        return new ObjectMapper().readValue(json, Datastore.class);
    }

    public static Datastore fromJSONFile(@Nonnull final File file) throws IOException {
        return new ObjectMapper().readValue(file, Datastore.class);
    }

}

package net.steveperkins.fitnessjiffy.etl.reader;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.model.Exercise;
import net.steveperkins.fitnessjiffy.etl.model.ExercisePerformed;
import net.steveperkins.fitnessjiffy.etl.model.Food;
import net.steveperkins.fitnessjiffy.etl.model.FoodEaten;
import net.steveperkins.fitnessjiffy.etl.model.User;
import net.steveperkins.fitnessjiffy.etl.model.Weight;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public abstract class JDBCReader {

    public interface TABLES {
        String USER = "FITNESSJIFFY_USER";
        String WEIGHT = "WEIGHT";
        String FOOD = "FOOD";
        String FOOD_EATEN = "FOOD_EATEN";
        String EXERCISE = "EXERCISE";
        String EXERCISE_PERFORMED = "EXERCISE_PERFORMED";
    }

    public interface USER {
        String ID = "ID";
        String GENDER = "GENDER";
        String BIRTHDATE = "BIRTHDATE";
        String HEIGHT_IN_INCHES = "HEIGHT_IN_INCHES";
        String ACTIVITY_LEVEL = "ACTIVITY_LEVEL";
        String EMAIL = "EMAIL";
        String PASSWORD_HASH = "PASSWORD_HASH";
        String FIRST_NAME = "FIRST_NAME";
        String LAST_NAME = "LAST_NAME";
        String CREATED_TIME = "CREATED_TIME";
        String LAST_UPDATED_TIME = "LAST_UPDATED_TIME";
    }

    public interface WEIGHT {
        String ID = "ID";
        String USER_ID = "USER_ID";
        String DATE = "DATE";
        String POUNDS = "POUNDS";
    }

    public interface FOOD {
        String ID = "ID";
        String USER_ID = "OWNER_ID";
        String NAME = "NAME";
        String DEFAULT_SERVING_TYPE = "DEFAULT_SERVING_TYPE";
        String SERVING_TYPE_QTY = "SERVING_TYPE_QTY";
        String CALORIES = "CALORIES";
        String FAT = "FAT";
        String SATURATED_FAT = "SATURATED_FAT";
        String CARBS = "CARBS";
        String FIBER = "FIBER";
        String SUGAR = "SUGAR";
        String PROTEIN = "PROTEIN";
        String SODIUM = "SODIUM";
        String CREATED_TIME = "CREATED_TIME";
        String LAST_UPDATED_TIME = "LAST_UPDATED_TIME";
    }

    public interface FOOD_EATEN {
        String ID = "ID";
        String USER_ID = "USER_ID";
        String FOOD_ID = "FOOD_ID";
        String DATE = "DATE";
        String SERVING_QTY = "SERVING_QTY";
        String SERVING_TYPE = "SERVING_TYPE";
    }

    public interface EXERCISE {
        String ID = "ID";
        String CATEGORY = "CATEGORY";
        String CODE = "CODE";
        String DESCRIPTION = "DESCRIPTION";
        String METABOLIC_EQUIVALENT = "METABOLIC_EQUIVALENT";
    }

    public interface EXERCISE_PERFORMED {
        String ID = "ID";
        String USER_ID = "USER_ID";
        String EXERCISE_ID = "EXERCISE_ID";
        String DATE = "DATE";
        String MINUTES = "MINUTES";
    }

    protected static final String EXERCISES_JSON_PATH = "/exercises.json";

    protected Connection connection;

    public JDBCReader(@Nonnull final Connection connection) {
        if (connection == null) {
            throw new NullPointerException();
        }
        this.connection = connection;
    }

    @Nonnull
    public Datastore read() throws Exception {
        if (connection.isClosed()) {
            throw new IllegalStateException();
        }
        final Datastore datastore = new Datastore();

        // Load exercises
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.EXERCISE);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                final Exercise exercise = new Exercise(
                        UUID.nameUUIDFromBytes(rs.getBytes(EXERCISE.ID)),
                        rs.getString(EXERCISE.CODE),
                        rs.getDouble(EXERCISE.METABOLIC_EQUIVALENT),
                        rs.getString(EXERCISE.CATEGORY),
                        rs.getString(EXERCISE.DESCRIPTION)
                );
                datastore.addExercise(exercise);
            }
        }

        // Load global foods
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.FOOD + " WHERE " + FOOD.USER_ID + " IS NULL");
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                final Food food = new Food(
                        UUID.nameUUIDFromBytes(rs.getBytes(FOOD.ID)),
                        rs.getString(FOOD.NAME),
                        Food.ServingType.fromString(rs.getString(FOOD.DEFAULT_SERVING_TYPE)),
                        rs.getDouble(FOOD.SERVING_TYPE_QTY),
                        rs.getInt(FOOD.CALORIES),
                        rs.getDouble(FOOD.FAT),
                        rs.getDouble(FOOD.SATURATED_FAT),
                        rs.getDouble(FOOD.CARBS),
                        rs.getDouble(FOOD.FIBER),
                        rs.getDouble(FOOD.SUGAR),
                        rs.getDouble(FOOD.PROTEIN),
                        rs.getDouble(FOOD.SODIUM),
                        rs.getTimestamp(FOOD.CREATED_TIME),
                        rs.getTimestamp(FOOD.LAST_UPDATED_TIME)
                );
                datastore.addGlobalFood(food);
            }
        }

        // Load users (includes weights, user-owned foods, foods eaten, and exercises performed)
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.USER);
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                datastore.addUser(readUser(rs, connection));
            }
        }

        return datastore;
    }

    @Nonnull
    protected User readUser(
            @Nonnull final ResultSet rs,
            @Nonnull final Connection connection
    ) throws Exception {
        final byte[] userId = rs.getBytes(USER.ID);

        // Weights
        final Set<Weight> weights = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.WEIGHT + " WHERE " + WEIGHT.USER_ID + " = ?")) {
            statement.setBytes(1, userId);
            try (ResultSet weightsResultSet = statement.executeQuery()) {
                while (weightsResultSet.next()) {
                    final Weight weight = new Weight(
                            UUID.nameUUIDFromBytes(weightsResultSet.getBytes(WEIGHT.ID)),
                            weightsResultSet.getDate(WEIGHT.DATE),
                            weightsResultSet.getDouble(WEIGHT.POUNDS)
                    );
                    weights.add(weight);
                }
            }
        }

        // User-owned foods
        final Set<Food> foods = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.FOOD + " WHERE " + FOOD.USER_ID + " = ?")) {
            statement.setBytes(1, userId);
            try (ResultSet userFoodResultSet = statement.executeQuery()) {
                while (userFoodResultSet.next()) {
                    final Food food = new Food(
                            UUID.nameUUIDFromBytes(userFoodResultSet.getBytes(FOOD.ID)),
                            userFoodResultSet.getString(FOOD.NAME),
                            Food.ServingType.fromString(userFoodResultSet.getString(FOOD.DEFAULT_SERVING_TYPE)),
                            userFoodResultSet.getDouble(FOOD.SERVING_TYPE_QTY),
                            userFoodResultSet.getInt(FOOD.CALORIES),
                            userFoodResultSet.getDouble(FOOD.FAT),
                            userFoodResultSet.getDouble(FOOD.SATURATED_FAT),
                            userFoodResultSet.getDouble(FOOD.CARBS),
                            userFoodResultSet.getDouble(FOOD.FIBER),
                            userFoodResultSet.getDouble(FOOD.SUGAR),
                            userFoodResultSet.getDouble(FOOD.PROTEIN),
                            userFoodResultSet.getDouble(FOOD.SODIUM),
                            userFoodResultSet.getTimestamp(FOOD.CREATED_TIME),
                            userFoodResultSet.getTimestamp(FOOD.LAST_UPDATED_TIME)
                    );
                    foods.add(food);
                }
            }
        }

        // Foods eaten
        final Set<FoodEaten> foodsEaten = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.FOOD_EATEN + " WHERE " + FOOD_EATEN.USER_ID + " = ?")) {
            statement.setBytes(1, userId);
            try (ResultSet foodsEatenResultSet = statement.executeQuery()) {
                while (foodsEatenResultSet.next()) {
                    final FoodEaten foodEaten = new FoodEaten(
                            UUID.nameUUIDFromBytes(foodsEatenResultSet.getBytes(FOOD_EATEN.ID)),
                            UUID.nameUUIDFromBytes(foodsEatenResultSet.getBytes(FOOD_EATEN.FOOD_ID)),
                            foodsEatenResultSet.getDate(FOOD_EATEN.DATE),
                            Food.ServingType.fromString(foodsEatenResultSet.getString(FOOD_EATEN.SERVING_TYPE)),
                            foodsEatenResultSet.getDouble(FOOD_EATEN.SERVING_QTY)
                    );
                    foodsEaten.add(foodEaten);
                }
            }
        }

        // Exercises performed
        final Set<ExercisePerformed> exercisesPerformed = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM " + TABLES.EXERCISE_PERFORMED + " WHERE " + TABLES.EXERCISE_PERFORMED + "." + EXERCISE_PERFORMED.USER_ID + " = ?")) {
            statement.setBytes(1, userId);
            try (ResultSet exercisesPerformedResultSet = statement.executeQuery()) {
                while (exercisesPerformedResultSet.next()) {
                    final ExercisePerformed exercisePerformed = new ExercisePerformed(
                            UUID.nameUUIDFromBytes(exercisesPerformedResultSet.getBytes(EXERCISE_PERFORMED.ID)),
                            UUID.nameUUIDFromBytes(exercisesPerformedResultSet.getBytes(EXERCISE_PERFORMED.EXERCISE_ID)),
                            exercisesPerformedResultSet.getDate(EXERCISE_PERFORMED.DATE),
                            exercisesPerformedResultSet.getInt(EXERCISE_PERFORMED.MINUTES)
                    );
                    exercisesPerformed.add(exercisePerformed);
                }
            }
        }

        return new User(
                UUID.nameUUIDFromBytes(userId),
                User.Gender.fromString(rs.getString(USER.GENDER)),
                rs.getDate(USER.BIRTHDATE),
                rs.getDouble(USER.HEIGHT_IN_INCHES),
                User.ActivityLevel.fromValue(rs.getDouble(USER.ACTIVITY_LEVEL)),
                rs.getString(USER.EMAIL),
                rs.getString(USER.PASSWORD_HASH),
                rs.getString(USER.FIRST_NAME),
                rs.getString(USER.LAST_NAME),
                rs.getTimestamp(USER.CREATED_TIME),
                rs.getTimestamp(USER.LAST_UPDATED_TIME),
                weights,
                foods,
                foodsEaten,
                exercisesPerformed
        );
    }

}

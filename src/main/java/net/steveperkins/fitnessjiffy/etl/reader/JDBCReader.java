package net.steveperkins.fitnessjiffy.etl.reader;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.model.Exercise;
import net.steveperkins.fitnessjiffy.etl.model.ExercisePerformed;
import net.steveperkins.fitnessjiffy.etl.model.Food;
import net.steveperkins.fitnessjiffy.etl.model.FoodEaten;
import net.steveperkins.fitnessjiffy.etl.model.User;
import net.steveperkins.fitnessjiffy.etl.model.Weight;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public abstract class JDBCReader {

    public interface TABLES {
        public static String USER = "FITNESSJIFFY_USER";
        public static String WEIGHT = "WEIGHT";
        public static String FOOD = "FOOD";
        public static String FOOD_EATEN = "FOOD_EATEN";
        public static String EXERCISE = "EXERCISE";
        public static String EXERCISE_PERFORMED = "EXERCISE_PERFORMED";
    }
    public interface USER {
        public static final String ID = "ID";
        public static final String GENDER = "GENDER";
        public static final String AGE = "AGE";
        public static final String HEIGHT_IN_INCHES = "HEIGHT_IN_INCHES";
        public static final String ACTIVITY_LEVEL = "ACTIVITY_LEVEL";
        public static final String USERNAME = "USERNAME";
        public static final String PASSWORD = "PASSWORD";
        public static final String FIRST_NAME= "FIRST_NAME";
        public static final String LAST_NAME = "LAST_NAME";
        public static final String IS_ACTIVE = "IS_ACTIVE";
    }
    public interface WEIGHT {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String DATE = "DATE";
        public static final String POUNDS = "POUNDS";
    }
    public interface FOOD {
        public static final String ID = "ID";
        public static final String USER_ID = "OWNER_ID";
        public static final String NAME = "NAME";
        public static final String DEFAULT_SERVING_TYPE = "DEFAULT_SERVING_TYPE";
        public static final String SERVING_TYPE_QTY = "SERVING_TYPE_QTY";
        public static final String CALORIES = "CALORIES";
        public static final String FAT = "FAT";
        public static final String SATURATED_FAT = "SATURATED_FAT";
        public static final String CARBS = "CARBS";
        public static final String FIBER = "FIBER";
        public static final String SUGAR = "SUGAR";
        public static final String PROTEIN = "PROTEIN";
        public static final String SODIUM = "SODIUM";
    }
    public interface FOOD_EATEN {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String FOOD_ID = "FOOD_ID";
        public static final String DATE = "DATE";
        public static final String SERVING_QTY = "SERVING_QTY";
        public static final String SERVING_TYPE = "SERVING_TYPE";
    }
    public interface EXERCISE {
        public static final String ID = "ID";
        public static final String CATEGORY = "CATEGORY";
        public static final String CODE = "CODE";
        public static final String DESCRIPTION = "DESCRIPTION";
        public static final String METABOLIC_EQUIVALENT = "METABOLIC_EQUIVALENT";
    }
    public interface EXERCISE_PERFORMED {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String EXERCISE_ID = "EXERCISE_ID";
        public static final String DATE = "DATE";
        public static final String MINUTES = "MINUTES";
    }

    protected static final String EXERCISES_JSON_PATH = "/exercises.json";

    protected Connection connection;

    public JDBCReader(Connection connection) {
        if(connection == null) throw new NullPointerException();
        this.connection = connection;
    }

    public Datastore read() throws Exception {
        if(connection.isClosed()) throw new IllegalStateException();
        Datastore datastore = new Datastore();

        // Load exercises
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+ TABLES.EXERCISE);
             ResultSet rs = statement.executeQuery() ) {
            while(rs.next()) {
                Exercise exercise = new Exercise(
                        UUID.nameUUIDFromBytes(rs.getBytes(EXERCISE.ID)),
                        rs.getString(EXERCISE.CODE),
                        rs.getDouble(EXERCISE.METABOLIC_EQUIVALENT),
                        rs.getString(EXERCISE.CATEGORY),
                        rs.getString(EXERCISE.DESCRIPTION)
                );
                datastore.getExercises().add(exercise);
            }
        }

        // Load global foods
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOOD+" WHERE "+FOOD.USER_ID+" IS NULL");
             ResultSet rs = statement.executeQuery() ) {
            while(rs.next()) {
                Food food = new Food(
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
                        rs.getDouble(FOOD.SODIUM)
                );
                datastore.getGlobalFoods().add(food);
            }
        }

        // Load users (includes weights, user-owned foods, foods eaten, and exercises performed)
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.USER);
             ResultSet rs = statement.executeQuery() ) {
            while(rs.next()) {
                datastore.getUsers().add(readUser(rs, connection));
            }
        }

        return datastore;
    }

    protected User readUser(ResultSet rs, Connection connection) throws Exception {
        byte[] userId = rs.getBytes(USER.ID);

        // Weights
        Set<Weight> weights = new HashSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.WEIGHT+" WHERE "+WEIGHT.USER_ID+" = ?") ) {
            statement.setBytes(1, userId);
            try ( ResultSet weightsResultSet = statement.executeQuery() ) {
                while(weightsResultSet.next()) {
                    Weight weight = new Weight(
                            UUID.nameUUIDFromBytes(weightsResultSet.getBytes(WEIGHT.ID)),
                            weightsResultSet.getDate(WEIGHT.DATE),
                            weightsResultSet.getDouble(WEIGHT.POUNDS)
                    );
                    weights.add(weight);
                }
            }
        }

        // User-owned foods
        Set<Food> foods = new HashSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOOD+" WHERE "+FOOD.USER_ID+" = ?") ) {
            statement.setBytes(1, userId);
            try ( ResultSet userFoodResultSet = statement.executeQuery() ) {
                while(userFoodResultSet.next()) {
                    Food food = new Food(
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
                            userFoodResultSet.getDouble(FOOD.SODIUM)
                    );
                    foods.add(food);
                }
            }
        }

        // Foods eaten
        Set<FoodEaten> foodsEaten = new HashSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOOD_EATEN+" WHERE "+FOOD_EATEN.USER_ID+" = ?") ) {
            statement.setBytes(1, userId);
            try ( ResultSet foodsEatenResultSet = statement.executeQuery() ) {
                while(foodsEatenResultSet.next()) {
                    FoodEaten foodEaten = new FoodEaten(
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
        Set<ExercisePerformed> exercisesPerformed = new HashSet<>();
        try ( PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM "+TABLES.EXERCISE_PERFORMED+" WHERE "+TABLES.EXERCISE_PERFORMED+"."+EXERCISE_PERFORMED.USER_ID+" = ?") ) {
            statement.setBytes(1, userId);
            try ( ResultSet exercisesPerformedResultSet = statement.executeQuery() ) {
                while(exercisesPerformedResultSet.next()) {
                    ExercisePerformed exercisePerformed = new ExercisePerformed(
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
                rs.getInt(USER.AGE),
                rs.getDouble(USER.HEIGHT_IN_INCHES),
                User.ActivityLevel.fromValue(rs.getDouble(USER.ACTIVITY_LEVEL)),
                rs.getString(USER.USERNAME),
                rs.getString(USER.PASSWORD),
                rs.getString(USER.FIRST_NAME),
                rs.getString(USER.LAST_NAME),
                rs.getBoolean(USER.IS_ACTIVE),
                weights,
                foods,
                foodsEaten,
                exercisesPerformed
        );
    }

}

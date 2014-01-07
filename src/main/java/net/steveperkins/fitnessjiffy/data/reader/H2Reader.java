package net.steveperkins.fitnessjiffy.data.reader;

import com.google.common.base.Preconditions;
import net.steveperkins.fitnessjiffy.data.model.Datastore;
import net.steveperkins.fitnessjiffy.data.model.Exercise;
import net.steveperkins.fitnessjiffy.data.model.ExercisePerformed;
import net.steveperkins.fitnessjiffy.data.model.Food;
import net.steveperkins.fitnessjiffy.data.model.FoodEaten;
import net.steveperkins.fitnessjiffy.data.model.User;
import net.steveperkins.fitnessjiffy.data.model.Weight;
import net.steveperkins.fitnessjiffy.data.util.NoNullsSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;

public class H2Reader extends JDBCReader {

    public H2Reader(Connection connection) {
        super(connection);
    }

    @Override
    public Datastore read() throws Exception {
        Preconditions.checkState(!connection.isClosed());

        Datastore datastore = new Datastore();

        // Load exercises
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+ TABLES.EXERCISES);
            ResultSet rs = statement.executeQuery() ) {
            while(rs.next()) {
                Exercise exercise = new Exercise(
                        UUID.nameUUIDFromBytes(rs.getBytes(EXERCISES.ID)),
                        rs.getString(EXERCISES.CODE),
                        rs.getDouble(EXERCISES.METABOLIC_EQUIVALENT),
                        rs.getString(EXERCISES.CATEGORY),
                        rs.getString(EXERCISES.DESCRIPTION)
                );
                datastore.getExercises().add(exercise);
            }
        }

        // Load global foods
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOODS+" WHERE "+FOODS.USER_ID+" IS NULL");
            ResultSet rs = statement.executeQuery() ) {
            while(rs.next()) {
                Food food = new Food(
                        UUID.nameUUIDFromBytes(rs.getBytes(FOODS.ID)),
                        rs.getString(FOODS.NAME),
                        Food.ServingType.fromString(rs.getString(FOODS.DEFAULT_SERVING_TYPE)),
                        rs.getDouble(FOODS.SERVING_TYPE_QTY),
                        rs.getInt(FOODS.CALORIES),
                        rs.getDouble(FOODS.FAT),
                        rs.getDouble(FOODS.SATURATED_FAT),
                        rs.getDouble(FOODS.CARBS),
                        rs.getDouble(FOODS.FIBER),
                        rs.getDouble(FOODS.SUGAR),
                        rs.getDouble(FOODS.PROTEIN),
                        rs.getDouble(FOODS.SODIUM)
                );
                datastore.getGlobalFoods().add(food);
            }
        }

        // Load users (includes weights, user-owned foods, foods eaten, and exercises performed)
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.USERS);
             ResultSet rs = statement.executeQuery() ) {
            while(rs.next()) {
                datastore.getUsers().add(readUser(rs, connection));
            }
        }

        return datastore;
    }

    private User readUser(ResultSet rs, Connection connection) throws SQLException {
//        System.out.println("String == " + rs.getString(USERS.ID));
//        System.out.println("Bytes == " + rs.getBytes(USERS.ID));
//        System.out.println("Hex String == " + DatatypeConverter.printHexBinary(rs.getBytes(USERS.ID)).toLowerCase());
//        String userIdHexString = DatatypeConverter.printHexBinary(rs.getBytes(USERS.ID)).toLowerCase();
//        byte[] userIdBytes = ByteBuffer.allocate(16).putLong(userId.getMostSignificantBits()).putLong(userId.getLeastSignificantBits()).array();
//        UUID foo = UUID.nameUUIDFromBytes(DatatypeConverter.parseHexBinary(userIdHexString));
        byte[] userId = rs.getBytes(USERS.ID);

        // Weights
        Set<Weight> weights = new NoNullsSet<>();
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
        Set<Food> foods = new NoNullsSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOODS+" WHERE "+FOODS.USER_ID+" = ?") ) {
            statement.setBytes(1, userId);
            try ( ResultSet userFoodResultSet = statement.executeQuery() ) {
                while(userFoodResultSet.next()) {
                    Food food = new Food(
                            UUID.nameUUIDFromBytes(userFoodResultSet.getBytes(FOODS.ID)),
                            userFoodResultSet.getString(FOODS.NAME),
                            Food.ServingType.fromString(userFoodResultSet.getString(FOODS.DEFAULT_SERVING_TYPE)),
                            userFoodResultSet.getDouble(FOODS.SERVING_TYPE_QTY),
                            userFoodResultSet.getInt(FOODS.CALORIES),
                            userFoodResultSet.getDouble(FOODS.FAT),
                            userFoodResultSet.getDouble(FOODS.SATURATED_FAT),
                            userFoodResultSet.getDouble(FOODS.CARBS),
                            userFoodResultSet.getDouble(FOODS.FIBER),
                            userFoodResultSet.getDouble(FOODS.SUGAR),
                            userFoodResultSet.getDouble(FOODS.PROTEIN),
                            userFoodResultSet.getDouble(FOODS.SODIUM)
                    );
                    foods.add(food);
                }
            }
        }

        // Foods eaten
        Set<FoodEaten> foodsEaten = new NoNullsSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOODS_EATEN+" WHERE "+FOODS_EATEN.USER_ID+" = ?") ) {
            statement.setBytes(1, userId);
            try ( ResultSet foodsEatenResultSet = statement.executeQuery() ) {
                while(foodsEatenResultSet.next()) {
                    FoodEaten foodEaten = new FoodEaten(
                            UUID.nameUUIDFromBytes(foodsEatenResultSet.getBytes(FOODS_EATEN.ID)),
                            UUID.nameUUIDFromBytes(foodsEatenResultSet.getBytes(FOODS_EATEN.FOOD_ID)),
                            foodsEatenResultSet.getDate(FOODS_EATEN.DATE),
                            Food.ServingType.fromString(foodsEatenResultSet.getString(FOODS_EATEN.SERVING_TYPE)),
                            foodsEatenResultSet.getDouble(FOODS_EATEN.SERVING_QTY)
                    );
                    foodsEaten.add(foodEaten);
                }
            }
        }

        // Exercises performed
        Set<ExercisePerformed> exercisesPerformed = new NoNullsSet<>();
        try ( PreparedStatement statement = connection.prepareStatement(
                "SELECT * FROM "+TABLES.EXERCISES_PERFORMED+" WHERE "+TABLES.EXERCISES_PERFORMED+"."+EXERCISES_PERFORMED.USER_ID+" = ?") ) {
            statement.setBytes(1, userId);
            try ( ResultSet exercisesPerformedResultSet = statement.executeQuery() ) {
                while(exercisesPerformedResultSet.next()) {
                    ExercisePerformed exercisePerformed = new ExercisePerformed(
                            UUID.nameUUIDFromBytes(exercisesPerformedResultSet.getBytes(EXERCISES_PERFORMED.ID)),
                            UUID.nameUUIDFromBytes(exercisesPerformedResultSet.getBytes(EXERCISES_PERFORMED.EXERCISE_ID)),
                            exercisesPerformedResultSet.getDate(EXERCISES_PERFORMED.DATE),
                            exercisesPerformedResultSet.getInt(EXERCISES_PERFORMED.MINUTES)
                    );
                    exercisesPerformed.add(exercisePerformed);
                }
            }
        }

        return new User(
                UUID.nameUUIDFromBytes(userId),
                User.Gender.fromString(rs.getString(USERS.GENDER)),
                rs.getInt(USERS.AGE),
                rs.getDouble(USERS.HEIGHT_IN_INCHES),
                User.ActivityLevel.fromValue(rs.getDouble(USERS.ACTIVITY_LEVEL)),
                rs.getString(USERS.USERNAME),
                rs.getString(USERS.PASSWORD),
                rs.getString(USERS.FIRST_NAME),
                rs.getString(USERS.LAST_NAME),
                rs.getBoolean(USERS.ACTIVE),
                weights,
                foods,
                foodsEaten,
                exercisesPerformed
        );
    }

}

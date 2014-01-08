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

    private User readUser(ResultSet rs, Connection connection) throws SQLException {
//        System.out.println("String == " + rs.getString(USER.ID));
//        System.out.println("Bytes == " + rs.getBytes(USER.ID));
//        System.out.println("Hex String == " + DatatypeConverter.printHexBinary(rs.getBytes(USER.ID)).toLowerCase());
//        String userIdHexString = DatatypeConverter.printHexBinary(rs.getBytes(USER.ID)).toLowerCase();
//        byte[] userIdBytes = ByteBuffer.allocate(16).putLong(userId.getMostSignificantBits()).putLong(userId.getLeastSignificantBits()).array();
//        UUID foo = UUID.nameUUIDFromBytes(DatatypeConverter.parseHexBinary(userIdHexString));
        byte[] userId = rs.getBytes(USER.ID);

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
        Set<FoodEaten> foodsEaten = new NoNullsSet<>();
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
        Set<ExercisePerformed> exercisesPerformed = new NoNullsSet<>();
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

package net.steveperkins.fitnessjiffy.data.reader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import net.steveperkins.fitnessjiffy.data.model.*;
import net.steveperkins.fitnessjiffy.data.util.NoNullsMap;
import net.steveperkins.fitnessjiffy.data.util.NoNullsSet;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.UUID;

public class LegacySQLiteReader extends JDBCReader {

    public static interface TABLES {
        public static String USERS = "USERS";
        public static String WEIGHT = "WEIGHT";
        public static String FOODS = "FOODS";
        public static String FOODS_EATEN = "FOODS_EATEN";
        public static String EXERCISES = "EXERCISES";
        public static String EXERCISES_PERFORMED = "EXERCISES_PERFORMED";
    }
    public static interface USERS {
        public static final String ID = "ID";
        public static final String GENDER = "GENDER";
        public static final String AGE = "AGE";
        public static final String HEIGHT_IN_INCHES = "HEIGHT_IN_INCHES";
        public static final String ACTIVITY_LEVEL = "ACTIVITY_LEVEL";
        public static final String USERNAME = "USERNAME";
        public static final String PASSWORD = "PASSWORD";
        public static final String FIRST_NAME= "FIRST_NAME";
        public static final String LAST_NAME = "LAST_NAME";
        public static final String ACTIVE = "ACTIVE";
    }
    public static interface WEIGHT {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String DATE = "DATE";
        public static final String POUNDS = "POUNDS";
    }
    public static interface FOODS {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
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
    public static interface FOODS_EATEN {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String FOOD_ID = "FOOD_ID";
        public static final String DATE = "DATE";
        public static final String SERVING_QTY = "SERVING_QTY";
        public static final String SERVING_TYPE = "SERVING_TYPE";
    }
    public static interface EXERCISES {
        public static final String ID = "ID";
        public static final String NAME = "NAME";
        public static final String CALORIES_PER_HOUR = "CALORIES_PER_HOUR";
        public static final String HIDDEN = "HIDDEN";
    }
    public static interface EXERCISES_PERFORMED {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String EXERCISE_ID = "EXERCISE_ID";
        public static final String DATE = "DATE";
        public static final String MINUTES = "MINUTES";
    }

    private final NoNullsMap<Integer, UUID> foodIds = new NoNullsMap<>();
    private final NoNullsMap<String, UUID> exerciseIds = new NoNullsMap<String, UUID>() {{
        put("Yoga, Hatha (32 years - 265 lbs)",                     UUID.fromString("6cf6d7de-abe9-4981-bea4-bb45c8d8088e"));  // code: 02150
        put("yoga, Hatha",                                          UUID.fromString("6cf6d7de-abe9-4981-bea4-bb45c8d8088e"));
        put("Elliptical machine (14 cal/min)",                      UUID.fromString("fa0faf6f-90d7-446f-b8ba-a27f5bc80e72"));  // 02048
        put("Elliptical trainer, moderate effort",                  UUID.fromString("fa0faf6f-90d7-446f-b8ba-a27f5bc80e72"));
        put("Bowling (32 years - 265 lbs)",                         UUID.fromString("543d3c3e-a1c2-4a6e-a31c-e6e0e12baa1f"));  // 15092
        put("bowling, indoor, bowling alley",                       UUID.fromString("543d3c3e-a1c2-4a6e-a31c-e6e0e12baa1f"));
        put("Racquetball (33 years - 250 lbs)",                     UUID.fromString("08d08760-8504-419d-9c36-e58119cc0387"));  // 15530
        put("racquetball, general (Taylor Code 470)",               UUID.fromString("08d08760-8504-419d-9c36-e58119cc0387"));
        put("Swimming (32 years - 265 lbs)",                        UUID.fromString("8eca5fb3-16ae-4fc0-8899-893436c2f629"));  // 18310
        put("swimming, leisurely, not lap swimming, general",       UUID.fromString("8eca5fb3-16ae-4fc0-8899-893436c2f629"));
        put("Basketball, shooting baskets (33 years - 250 lbs)",    UUID.fromString("9a0de32d-ee1f-4370-848b-ef7c08379b03"));  // 15070
        put("basketball, shooting baskets",                         UUID.fromString("9a0de32d-ee1f-4370-848b-ef7c08379b03"));
        put("Weight lifting (33 years - 250 lbs)",                  UUID.fromString("7b4e54b2-a28e-4555-8945-3e7e13f9b659"));  // 02050
        put("resistance training (weight lifting, free weight, nautilus or universal), power lifting or body building, vigorous effort (Taylor Code 210)",
                UUID.fromString("7b4e54b2-a28e-4555-8945-3e7e13f9b659"));
        put("Walking, 3 mph (33 years - 250 lbs)",                  UUID.fromString("e3a850cc-a432-4709-bf0b-a06b7fd78c8e"));  // 17190
        put("walking, 2.8 to 3.2 mph, level, moderate pace, firm surface",
                UUID.fromString("e3a850cc-a432-4709-bf0b-a06b7fd78c8e"));
        put("Elliptical machine (13 cal/min)",                      UUID.fromString("fa0faf6f-90d7-446f-b8ba-a27f5bc80e72"));  // 02048
        put("Elliptical trainer, moderate effort",                  UUID.fromString("fa0faf6f-90d7-446f-b8ba-a27f5bc80e72"));
        put("Yoga, Hatha (33 years - 250 lbs)",                     UUID.fromString("6cf6d7de-abe9-4981-bea4-bb45c8d8088e"));  // 02150
        put("yoga, Hatha",                                          UUID.fromString("6cf6d7de-abe9-4981-bea4-bb45c8d8088e"));
        put("Bowling (33 years - 250 lbs)",                         UUID.fromString("543d3c3e-a1c2-4a6e-a31c-e6e0e12baa1f"));  // 15092
        put("bowling, indoor, bowling alley",                       UUID.fromString("543d3c3e-a1c2-4a6e-a31c-e6e0e12baa1f"));
        put("Stair climber (33 years - 240 lbs)",                   UUID.fromString("9f0f45a2-d3bd-4776-b62e-8e1cce0f0daa"));  // 02065
        put("stair-treadmill ergometer, general",                   UUID.fromString("9f0f45a2-d3bd-4776-b62e-8e1cce0f0daa"));
        put("Golf driving range (33 years - 240 lbs)",              UUID.fromString("848bf262-da21-48a8-96f3-53a1d9a0ef9e"));  // 15270
        put("golf, miniature, driving range",                       UUID.fromString("848bf262-da21-48a8-96f3-53a1d9a0ef9e"));
        put("Mowing lawn, reel mower (33 years - 240 lbs)",         UUID.fromString("41a7437c-a8ae-45d3-85d7-314c5b911bdf"));  // 08110
        put("mowing lawn, walk, hand mower (Taylor Code 570)",      UUID.fromString("41a7437c-a8ae-45d3-85d7-314c5b911bdf"));
        put("Yard work (33 years - 240 lbs)",                       UUID.fromString("8e32c657-c7ba-4e8a-b44b-bf5eef49dddc"));  // 08261
        put("yard work, general, moderate effort",                  UUID.fromString("8e32c657-c7ba-4e8a-b44b-bf5eef49dddc"));
        put("Bicycling, 10 mph (34 years - 280 lbs)",               UUID.fromString("31893938-9ef6-47f0-a362-0813940dbcf9"));  // 01020
        put("bicycling, 10-11.9 mph, leisure, slow, light effort",  UUID.fromString("31893938-9ef6-47f0-a362-0813940dbcf9"));
    }};
    private final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public LegacySQLiteReader(Connection connection) {
        super(connection);
    }

    @Override
    public Datastore read() throws Exception {
        Preconditions.checkState(!connection.isClosed());

        Datastore datastore = new Datastore();

        // Load exercises
        InputStream exerciseJsonStream = this.getClass().getResourceAsStream(EXERCISES_JSON_PATH);
        NoNullsSet<Exercise> exercises = new ObjectMapper().readValue(exerciseJsonStream, new TypeReference<NoNullsSet<Exercise>>() {});
        for(Exercise exercise : exercises) exercise.setDescription(exercise.getDescription().trim());
        datastore.getExercises().addAll(exercises);

        // Load global foods
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOODS+" WHERE "+FOODS.USER_ID+" IS NULL");
             ResultSet rs = statement.executeQuery(); ) {
            while(rs.next()) {
                datastore.getGlobalFoods().add(readFood(rs));
            }
        }

        // Load users (includes weights, user-owned foods, foods eaten, and exercises performed)
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.USERS);
             ResultSet rs = statement.executeQuery(); ) {
            while(rs.next()) {
                datastore.getUsers().add(readUser(rs, connection));
            }
        }
        return datastore;
    }

    private User readUser(ResultSet rs, Connection connection) throws Exception {
        User user = new User();

        // ID
        int id = rs.getInt(USERS.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed user, no ID");
        UUID uuid = UUID.randomUUID();
        user.setId(uuid);

        // Gender
        User.Gender gender = User.Gender.fromString(rs.getString(USERS.GENDER));
        if (gender == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no gender");
        user.setGender(gender);

        // Age
        int age = rs.getInt(USERS.AGE);
        if (age == 0 || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no age");
        user.setAge(age);

        // Height
        double heightInInches = rs.getDouble(USERS.HEIGHT_IN_INCHES);
        if (heightInInches == 0 || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no height");
        user.setHeightInInches(heightInInches);

        // Activity Level
        User.ActivityLevel activityLevel = User.ActivityLevel.fromValue(rs.getDouble(USERS.ACTIVITY_LEVEL));
        if (activityLevel == null || rs.wasNull())
            throw new Exception("Malformed user with ID: " + id + ", no activity level");
        user.setActivityLevel(activityLevel);

        // Username
        String username = rs.getString(USERS.USERNAME);
        if (username == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no username");
        user.setUsername(username);

        // Password
        String password = rs.getString(USERS.PASSWORD);
        if (password == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no password");
        user.setPassword(password);

        // First name
        String firstName = rs.getString(USERS.FIRST_NAME);
        if (firstName == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no first name");
        user.setFirstName(firstName);

        // Last name
        String lastName = rs.getString(USERS.LAST_NAME);
        if (lastName == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no last name");
        user.setLastName(firstName);

        // "Is Active?" flag
        String isActive = rs.getString(USERS.ACTIVE);
        if (isActive == null || (!isActive.trim().equalsIgnoreCase("Y") && !isActive.trim().equalsIgnoreCase("N")))
            throw new Exception("Malformed user with ID: " + id + ", no active flag");
        user.setActive(isActive.trim().equalsIgnoreCase("Y"));

        // Weights
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.WEIGHT+" WHERE "+WEIGHT.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet weightsResultSet = statement.executeQuery(); ) {
                while(weightsResultSet.next()) {
                    user.getWeights().add(readWeight(weightsResultSet));
                }
            }
        }

        // User-owned foods
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOODS+" WHERE "+FOODS.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet userFoodResultSet = statement.executeQuery(); ) {
                while(userFoodResultSet.next()) {
                    user.getFoods().add(readFood(userFoodResultSet));
                }
            }
        }

        // Foods eaten
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOODS_EATEN+" WHERE "+FOODS_EATEN.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet foodsEatenResultSet = statement.executeQuery(); ) {
                while(foodsEatenResultSet.next()) {
                    user.getFoodsEaten().add(readFoodEaten(foodsEatenResultSet));
                }
            }
        }

        // Exercises performed
        try ( PreparedStatement statement = connection.prepareStatement(
                "SELECT "+TABLES.EXERCISES_PERFORMED+".*, "+TABLES.EXERCISES+"."+EXERCISES.NAME
                        +" FROM "+TABLES.EXERCISES_PERFORMED+", "+TABLES.EXERCISES
                        +" WHERE "+TABLES.EXERCISES_PERFORMED+"."+EXERCISES_PERFORMED.EXERCISE_ID+" = "+TABLES.EXERCISES+"."+EXERCISES.ID
                        +" AND "+TABLES.EXERCISES_PERFORMED+"."+EXERCISES_PERFORMED.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet exercisesPerformedResultSet = statement.executeQuery(); ) {
                while(exercisesPerformedResultSet.next()) {
                    user.getExercisesPerformed().add(readExercisePerformed(exercisesPerformedResultSet));
                }
            }
        }

        return user;
    }

    private Weight readWeight(ResultSet rs) throws Exception {
        Weight weight = new Weight();

        // ID
        int id = rs.getInt(WEIGHT.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed weight, no ID");
        UUID uuid = UUID.randomUUID();
        weight.setId(uuid);

        // Date
        String date = rs.getString(WEIGHT.DATE);
        if(date == null || rs.wasNull()) throw new Exception("Malformed weight with ID: " + id + ", no date");
        weight.setDate(dateFormatter.parse(date));

        // Pounds
        Double pounds = rs.getDouble(WEIGHT.POUNDS);
        if(pounds == null || rs.wasNull()) throw new Exception("Malformed weight with ID: " + id + ", no pounds");
        weight.setPounds(pounds);

        return weight;
    }

    private Food readFood(ResultSet rs) throws Exception {
        Food food = new Food();

        // ID
        int id = rs.getInt(FOODS.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed food, no ID");
        UUID uuid = UUID.randomUUID();
        foodIds.put(id, uuid);
        food.setId(uuid);

        // Name
        String name = rs.getString(FOODS.NAME);
        if(name == null || rs.wasNull()) throw new Exception("Malformed food with ID: " + id + ", no name");
        food.setName(name);

        // Default serving type
        Food.ServingType defaultServingType = Food.ServingType.fromString(rs.getString(FOODS.DEFAULT_SERVING_TYPE));
        if(defaultServingType == null || rs.wasNull()) throw new Exception("Malformed food with ID: " + id + ", no default serving type");
        food.setDefaultServingType(defaultServingType);

        // Serving type qty
        double servingTypeQty = rs.getDouble(FOODS.SERVING_TYPE_QTY);
        if(servingTypeQty == 0 || rs.wasNull()) throw new Exception("Malformed food with ID: " + id + ", no serving type qty");
        food.setServingTypeQty(servingTypeQty);

        //
        // The remaining attributes of a Food might actually be zero, so we don't treat
        // it as an error condition.
        //

        // Calories
        int calories = rs.getInt(FOODS.CALORIES);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null calories");
        }
        food.setCalories(calories);

        // Fat
        double fat = rs.getDouble(FOODS.FAT);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null fat");
        }
        food.setFat(fat);

        // Saturated fat
        double saturatedFat = rs.getDouble(FOODS.SATURATED_FAT);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null saturated fat");
        }
        food.setSaturatedFat(saturatedFat);

        // Carbs
        double carbs = rs.getDouble(FOODS.CARBS);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null carbs");
        }
        food.setCarbs(carbs);

        // Fiber
        double fiber = rs.getDouble(FOODS.FIBER);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null fiber");
        }
        food.setFiber(fiber);

        // Sugar
        double sugar = rs.getDouble(FOODS.SUGAR);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null sugar");
        }
        food.setSugar(sugar);

        // Protein
        double protein = rs.getDouble(FOODS.PROTEIN);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null protein");
        }
        food.setProtein(protein);

        // Sodium
        double sodium = rs.getDouble(FOODS.SODIUM);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null sodium");
        }
        food.setSodium(sodium);

        return food;
    }

    private FoodEaten readFoodEaten(ResultSet rs) throws Exception {
        FoodEaten foodEaten = new FoodEaten();

        // ID
        int id = rs.getInt(FOODS_EATEN.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed food eaten, no ID");
        UUID uuid = UUID.randomUUID();
        foodEaten.setId(uuid);

        // Food ID
        int foodId = rs.getInt(FOODS_EATEN.FOOD_ID);
        if (foodId == 0 || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no food ID");
        if(foodIds.get(foodId) == null) throw new Exception("Food eaten with ID: " + id + " references unknown food ID: " + foodId);
        foodEaten.setFoodId(foodIds.get(foodId));

        // Date
        String date = rs.getString(FOODS_EATEN.DATE);
        if(date == null || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no date");
        foodEaten.setDate(dateFormatter.parse(date));

        // Serving Qty
        double servingQty = rs.getDouble(FOODS_EATEN.SERVING_QTY);
        if(servingQty == 0 || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no serving qty");
        foodEaten.setServingQty(servingQty);

        // Serving type
        Food.ServingType servingType = Food.ServingType.fromString(rs.getString(FOODS_EATEN.SERVING_TYPE));
        if(servingType == null || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no serving type");
        foodEaten.setServingType(servingType);

        return foodEaten;
    }

    private ExercisePerformed readExercisePerformed(ResultSet rs) throws Exception {
        ExercisePerformed exercisePerformed = new ExercisePerformed();

        // ID
        int id = rs.getInt(EXERCISES_PERFORMED.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed exercise performed, no ID");
        UUID uuid = UUID.randomUUID();
        exercisePerformed.setId(uuid);

        // Exercise ID
        String legacyName = rs.getString(EXERCISES.NAME);
        if(legacyName == null || rs.wasNull()) throw new Exception("Malformed exercise performed with ID: " + id + ", found no exercise name");
        legacyName = legacyName.replace("\u00A0", " ").trim();  // For some reason, String.trim() doesn't remove ASCII character 160
        UUID exerciseId = exerciseIds.get(legacyName);
        if(exerciseId == null) {
            throw new Exception("Malformed exercise performed with ID: " + id + ", found no new id matching legacy name: [" + legacyName + "]");
        }
        exercisePerformed.setExerciseId(exerciseId);

        // Date
        String date = rs.getString(EXERCISES_PERFORMED.DATE);
        if(date == null || rs.wasNull()) throw new Exception("Malformed exercise performed with ID: " + id + ", no date");
        exercisePerformed.setDate(dateFormatter.parse(date));

        // Minutes
        int minutes = rs.getInt(EXERCISES_PERFORMED.MINUTES);
        if(minutes == 0 || rs.wasNull()) throw new Exception("Malformed exercise performed with ID: " + id + ", no minutes");
        exercisePerformed.setMinutes(minutes);

        return exercisePerformed;
    }

}

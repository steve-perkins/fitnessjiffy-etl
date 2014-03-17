package net.steveperkins.fitnessjiffy.etl.reader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.model.Exercise;
import net.steveperkins.fitnessjiffy.etl.model.ExercisePerformed;
import net.steveperkins.fitnessjiffy.etl.model.Food;
import net.steveperkins.fitnessjiffy.etl.model.FoodEaten;
import net.steveperkins.fitnessjiffy.etl.model.User;
import net.steveperkins.fitnessjiffy.etl.model.Weight;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class LegacySQLiteReader extends JDBCReader {

    public interface TABLES extends JDBCReader.TABLES {
        public static final String USER = "USERS";
        public static final String FOOD = "FOODS";
        public static final String FOOD_EATEN = "FOODS_EATEN";
        public static final String EXERCISE = "EXERCISES";
        public static final String EXERCISE_PERFORMED = "EXERCISES_PERFORMED";
    }
    public interface USER extends JDBCReader.USER {
        public static final String IS_ACTIVE = "ACTIVE";
    }
    public interface WEIGHT {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String DATE = "DATE";
        public static final String POUNDS = "POUNDS";
    }
    public interface FOOD extends JDBCReader.FOOD {
        public static final String USER_ID = "USER_ID";
    }
    public interface FOOD_EATEN extends JDBCReader.FOOD_EATEN {
    }
    public interface EXERCISE {
        public static final String ID = "ID";
        public static final String NAME = "NAME";
        public static final String CALORIES_PER_HOUR = "CALORIES_PER_HOUR";
        public static final String HIDDEN = "HIDDEN";
    }
    public interface EXERCISE_PERFORMED extends JDBCReader.EXERCISE_PERFORMED {
    }

    private final Map<Integer, UUID> foodIds = new HashMap<>();
    private final Map<String, UUID> exerciseIds = new HashMap<String, UUID>() {{
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
        if(connection.isClosed()) throw new IllegalStateException();

        Datastore datastore = new Datastore();

        // Load exercises
        InputStream exerciseJsonStream = this.getClass().getResourceAsStream(EXERCISES_JSON_PATH);
        Set<Exercise> exercises = new ObjectMapper().readValue(exerciseJsonStream, new TypeReference<Set<Exercise>>() {});
        for(Exercise exercise : exercises) exercise.setDescription(exercise.getDescription().trim());
        datastore.getExercises().addAll(exercises);

        // Load global foods
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOOD+" WHERE "+FOOD.USER_ID+" IS NULL");
             ResultSet rs = statement.executeQuery() ) {
            while(rs.next()) {
                datastore.getGlobalFoods().add(readFood(rs));
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

    @Override
    protected User readUser(ResultSet rs, Connection connection) throws Exception {
        int id = rs.getInt(USER.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed user, no ID");
        UUID uuid = UUID.randomUUID();

        User.Gender gender = User.Gender.fromString(rs.getString(USER.GENDER));
        if (gender == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no gender");

        int age = rs.getInt(USER.AGE);
        if (age == 0 || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no age");

        double heightInInches = rs.getDouble(USER.HEIGHT_IN_INCHES);
        if (heightInInches == 0 || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no height");

        User.ActivityLevel activityLevel = User.ActivityLevel.fromValue(rs.getDouble(USER.ACTIVITY_LEVEL));
        if (activityLevel == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no activity level");

        String username = rs.getString(USER.USERNAME);
        if (username == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no username");

        String password = rs.getString(USER.PASSWORD);
        if (password == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no password");

        String firstName = rs.getString(USER.FIRST_NAME);
        if (firstName == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no first name");

        String lastName = rs.getString(USER.LAST_NAME);
        if (lastName == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no last name");

        String isActive = rs.getString(USER.IS_ACTIVE);
        if (isActive == null || (!isActive.trim().equalsIgnoreCase("Y") && !isActive.trim().equalsIgnoreCase("N")))
            throw new Exception("Malformed user with ID: " + id + ", no active flag");

        // Weights
        Set<Weight> weights = new HashSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.WEIGHT+" WHERE "+WEIGHT.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet weightsResultSet = statement.executeQuery() ) {
                while(weightsResultSet.next()) {
                    weights.add(readWeight(weightsResultSet));
                }
            }
        }

        // User-owned foods
        Set<Food> foods = new HashSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOOD+" WHERE "+FOOD.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet userFoodResultSet = statement.executeQuery() ) {
                while(userFoodResultSet.next()) {
                    foods.add(readFood(userFoodResultSet));
                }
            }
        }

        // Foods eaten
        Set<FoodEaten> foodsEaten = new HashSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOOD_EATEN+" WHERE "+FOOD_EATEN.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet foodsEatenResultSet = statement.executeQuery() ) {
                while(foodsEatenResultSet.next()) {
                    foodsEaten.add(readFoodEaten(foodsEatenResultSet));
                }
            }
        }

        // Exercises performed
        Set<ExercisePerformed> exercisesPerformed = new HashSet<>();
        try ( PreparedStatement statement = connection.prepareStatement(
                "SELECT "+TABLES.EXERCISE_PERFORMED+".*, "+TABLES.EXERCISE+"."+EXERCISE.NAME
                        +" FROM "+TABLES.EXERCISE_PERFORMED+", "+TABLES.EXERCISE
                        +" WHERE "+TABLES.EXERCISE_PERFORMED+"."+EXERCISE_PERFORMED.EXERCISE_ID+" = "+TABLES.EXERCISE+"."+EXERCISE.ID
                        +" AND "+TABLES.EXERCISE_PERFORMED+"."+EXERCISE_PERFORMED.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet exercisesPerformedResultSet = statement.executeQuery() ) {
                while(exercisesPerformedResultSet.next()) {
                    exercisesPerformed.add(readExercisePerformed(exercisesPerformedResultSet));
                }
            }
        }

        return new User(
                uuid,
                gender,
                age,
                heightInInches,
                activityLevel,
                username,
                password,
                firstName,
                lastName,
                isActive.trim().equalsIgnoreCase("Y"),
                weights,
                foods,
                foodsEaten,
                exercisesPerformed
        );
    }

    private Weight readWeight(ResultSet rs) throws Exception {
        int id = rs.getInt(WEIGHT.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed weight, no ID");
        UUID uuid = UUID.randomUUID();

        String date = rs.getString(WEIGHT.DATE);
        if(date == null || rs.wasNull()) throw new Exception("Malformed weight with ID: " + id + ", no date");

        Double pounds = rs.getDouble(WEIGHT.POUNDS);
        if(pounds == null || rs.wasNull()) throw new Exception("Malformed weight with ID: " + id + ", no pounds");

        return new Weight(
                uuid,
                new Date(dateFormatter.parse(date).getTime()),
                pounds
        );
    }

    private Food readFood(ResultSet rs) throws Exception {
        int id = rs.getInt(FOOD.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed food, no ID");
        UUID uuid = UUID.randomUUID();
        foodIds.put(id, uuid);

        String name = rs.getString(FOOD.NAME);
        if(name == null || rs.wasNull()) throw new Exception("Malformed food with ID: " + id + ", no name");

        Food.ServingType defaultServingType = Food.ServingType.fromString(rs.getString(FOOD.DEFAULT_SERVING_TYPE));
        if(defaultServingType == null || rs.wasNull()) throw new Exception("Malformed food with ID: " + id + ", no default serving type");

        double servingTypeQty = rs.getDouble(FOOD.SERVING_TYPE_QTY);
        if(servingTypeQty == 0 || rs.wasNull()) throw new Exception("Malformed food with ID: " + id + ", no serving type qty");

        //
        // The remaining attributes of a Food might actually be zero, so we don't treat
        // it as an error condition.
        //

        int calories = rs.getInt(FOOD.CALORIES);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null calories");
        }

        double fat = rs.getDouble(FOOD.FAT);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null fat");
        }

        double saturatedFat = rs.getDouble(FOOD.SATURATED_FAT);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null saturated fat");
        }

        double carbs = rs.getDouble(FOOD.CARBS);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null carbs");
        }

        double fiber = rs.getDouble(FOOD.FIBER);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null fiber");
        }

        double sugar = rs.getDouble(FOOD.SUGAR);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null sugar");
        }

        double protein = rs.getDouble(FOOD.PROTEIN);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null protein");
        }

        double sodium = rs.getDouble(FOOD.SODIUM);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null sodium");
        }

        return new Food(
                uuid,
                name,
                defaultServingType,
                servingTypeQty,
                calories,
                fat,
                saturatedFat,
                carbs,
                fiber,
                sugar,
                protein,
                sodium
        );
    }

    private FoodEaten readFoodEaten(ResultSet rs) throws Exception {
        int id = rs.getInt(FOOD_EATEN.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed food eaten, no ID");
        UUID uuid = UUID.randomUUID();

        int foodId = rs.getInt(FOOD_EATEN.FOOD_ID);
        if (foodId == 0 || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no food ID");
        if(foodIds.get(foodId) == null) throw new Exception("Food eaten with ID: " + id + " references unknown food ID: " + foodId);

        String date = rs.getString(FOOD_EATEN.DATE);
        if(date == null || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no date");

        double servingQty = rs.getDouble(FOOD_EATEN.SERVING_QTY);
        if(servingQty == 0 || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no serving qty");

        Food.ServingType servingType = Food.ServingType.fromString(rs.getString(FOOD_EATEN.SERVING_TYPE));
        if(servingType == null || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no serving type");

        return new FoodEaten(
                uuid,
                foodIds.get(foodId),
                new Date(dateFormatter.parse(date).getTime()),
                servingType,
                servingQty
        );
    }

    private ExercisePerformed readExercisePerformed(ResultSet rs) throws Exception {
        int id = rs.getInt(EXERCISE_PERFORMED.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed exercise performed, no ID");
        UUID uuid = UUID.randomUUID();

        // Exercise ID
        String legacyName = rs.getString(EXERCISE.NAME);
        if(legacyName == null || rs.wasNull()) throw new Exception("Malformed exercise performed with ID: " + id + ", found no exercise name");
        legacyName = legacyName.replace("\u00A0", " ").trim();  // For some reason, String.trim() doesn't remove ASCII character 160
        UUID exerciseId = exerciseIds.get(legacyName);
        if(exerciseId == null) {
            throw new Exception("Malformed exercise performed with ID: " + id + ", found no new id matching legacy name: [" + legacyName + "]");
        }

        // Date
        String date = rs.getString(EXERCISE_PERFORMED.DATE);
        if(date == null || rs.wasNull()) throw new Exception("Malformed exercise performed with ID: " + id + ", no date");

        // Minutes
        int minutes = rs.getInt(EXERCISE_PERFORMED.MINUTES);
        if(minutes == 0 || rs.wasNull()) throw new Exception("Malformed exercise performed with ID: " + id + ", no minutes");

        return new ExercisePerformed(
                uuid,
                exerciseId,
                new Date(dateFormatter.parse(date).getTime()),
                minutes
        );
    }

}

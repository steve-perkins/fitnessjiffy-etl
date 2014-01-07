package net.steveperkins.fitnessjiffy.data.reader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import net.steveperkins.fitnessjiffy.data.model.Datastore;
import net.steveperkins.fitnessjiffy.data.model.Exercise;
import net.steveperkins.fitnessjiffy.data.model.ExercisePerformed;
import net.steveperkins.fitnessjiffy.data.model.Food;
import net.steveperkins.fitnessjiffy.data.model.FoodEaten;
import net.steveperkins.fitnessjiffy.data.model.User;
import net.steveperkins.fitnessjiffy.data.model.Weight;
import net.steveperkins.fitnessjiffy.data.util.NoNullsMap;
import net.steveperkins.fitnessjiffy.data.util.NoNullsSet;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.UUID;

public class LegacySQLiteReader extends JDBCReader {

    public interface TABLES extends JDBCReader.TABLES {
        public static final String USERS = "USERS";
        public static final String FOODS = "FOODS";
        public static final String FOODS_EATEN = "FOODS_EATEN";
        public static final String EXERCISES = "EXERCISES";
        public static final String EXERCISES_PERFORMED = "EXERCISES_PERFORMED";
    }
    public interface USERS extends JDBCReader.USERS {
        public static final String ACTIVE = "ACTIVE";
    }
    public interface WEIGHT {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String DATE = "DATE";
        public static final String POUNDS = "POUNDS";
    }
    public interface FOODS extends JDBCReader.FOODS {
        public static final String USER_ID = "USER_ID";
    }
    public interface FOODS_EATEN extends JDBCReader.FOODS_EATEN {
    }
    public interface EXERCISES extends JDBCReader.EXERCISES {
        public static final String NAME = "NAME";
        public static final String CALORIES_PER_HOUR = "CALORIES_PER_HOUR";
        public static final String HIDDEN = "HIDDEN";

        public static final String CATEGORY = null;
        public static final String CODE = null;
        public static final String DESCRIPTION = null;
        public static final String METABOLIC_EQUIVALENT = null;
    }
    public interface EXERCISES_PERFORMED extends JDBCReader.EXERCISES_PERFORMED {
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
             ResultSet rs = statement.executeQuery() ) {
            while(rs.next()) {
                datastore.getGlobalFoods().add(readFood(rs));
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

    private User readUser(ResultSet rs, Connection connection) throws Exception {
        int id = rs.getInt(USERS.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed user, no ID");
        UUID uuid = UUID.randomUUID();

        User.Gender gender = User.Gender.fromString(rs.getString(USERS.GENDER));
        if (gender == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no gender");

        int age = rs.getInt(USERS.AGE);
        if (age == 0 || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no age");

        double heightInInches = rs.getDouble(USERS.HEIGHT_IN_INCHES);
        if (heightInInches == 0 || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no height");

        User.ActivityLevel activityLevel = User.ActivityLevel.fromValue(rs.getDouble(USERS.ACTIVITY_LEVEL));
        if (activityLevel == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no activity level");

        String username = rs.getString(USERS.USERNAME);
        if (username == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no username");

        String password = rs.getString(USERS.PASSWORD);
        if (password == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no password");

        String firstName = rs.getString(USERS.FIRST_NAME);
        if (firstName == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no first name");

        String lastName = rs.getString(USERS.LAST_NAME);
        if (lastName == null || rs.wasNull()) throw new Exception("Malformed user with ID: " + id + ", no last name");

        String isActive = rs.getString(USERS.ACTIVE);
        if (isActive == null || (!isActive.trim().equalsIgnoreCase("Y") && !isActive.trim().equalsIgnoreCase("N")))
            throw new Exception("Malformed user with ID: " + id + ", no active flag");

        // Weights
        Set<Weight> weights = new NoNullsSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.WEIGHT+" WHERE "+WEIGHT.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet weightsResultSet = statement.executeQuery() ) {
                while(weightsResultSet.next()) {
                    weights.add(readWeight(weightsResultSet));
                }
            }
        }

        // User-owned foods
        Set<Food> foods = new NoNullsSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOODS+" WHERE "+FOODS.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet userFoodResultSet = statement.executeQuery() ) {
                while(userFoodResultSet.next()) {
                    foods.add(readFood(userFoodResultSet));
                }
            }
        }

        // Foods eaten
        Set<FoodEaten> foodsEaten = new NoNullsSet<>();
        try ( PreparedStatement statement = connection.prepareStatement("SELECT * FROM "+TABLES.FOODS_EATEN+" WHERE "+FOODS_EATEN.USER_ID+" = ?") ) {
            statement.setInt(1, id);
            try ( ResultSet foodsEatenResultSet = statement.executeQuery() ) {
                while(foodsEatenResultSet.next()) {
                    foodsEaten.add(readFoodEaten(foodsEatenResultSet));
                }
            }
        }

        // Exercises performed
        Set<ExercisePerformed> exercisesPerformed = new NoNullsSet<>();
        try ( PreparedStatement statement = connection.prepareStatement(
                "SELECT "+TABLES.EXERCISES_PERFORMED+".*, "+TABLES.EXERCISES+"."+EXERCISES.NAME
                        +" FROM "+TABLES.EXERCISES_PERFORMED+", "+TABLES.EXERCISES
                        +" WHERE "+TABLES.EXERCISES_PERFORMED+"."+EXERCISES_PERFORMED.EXERCISE_ID+" = "+TABLES.EXERCISES+"."+EXERCISES.ID
                        +" AND "+TABLES.EXERCISES_PERFORMED+"."+EXERCISES_PERFORMED.USER_ID+" = ?") ) {
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
                dateFormatter.parse(date),
                pounds
        );
    }

    private Food readFood(ResultSet rs) throws Exception {
        int id = rs.getInt(FOODS.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed food, no ID");
        UUID uuid = UUID.randomUUID();
        foodIds.put(id, uuid);

        String name = rs.getString(FOODS.NAME);
        if(name == null || rs.wasNull()) throw new Exception("Malformed food with ID: " + id + ", no name");

        Food.ServingType defaultServingType = Food.ServingType.fromString(rs.getString(FOODS.DEFAULT_SERVING_TYPE));
        if(defaultServingType == null || rs.wasNull()) throw new Exception("Malformed food with ID: " + id + ", no default serving type");

        double servingTypeQty = rs.getDouble(FOODS.SERVING_TYPE_QTY);
        if(servingTypeQty == 0 || rs.wasNull()) throw new Exception("Malformed food with ID: " + id + ", no serving type qty");

        //
        // The remaining attributes of a Food might actually be zero, so we don't treat
        // it as an error condition.
        //

        int calories = rs.getInt(FOODS.CALORIES);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null calories");
        }

        double fat = rs.getDouble(FOODS.FAT);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null fat");
        }

        double saturatedFat = rs.getDouble(FOODS.SATURATED_FAT);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null saturated fat");
        }

        double carbs = rs.getDouble(FOODS.CARBS);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null carbs");
        }

        double fiber = rs.getDouble(FOODS.FIBER);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null fiber");
        }

        double sugar = rs.getDouble(FOODS.SUGAR);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null sugar");
        }

        double protein = rs.getDouble(FOODS.PROTEIN);
        if(rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null protein");
        }

        double sodium = rs.getDouble(FOODS.SODIUM);
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
        int id = rs.getInt(FOODS_EATEN.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed food eaten, no ID");
        UUID uuid = UUID.randomUUID();

        int foodId = rs.getInt(FOODS_EATEN.FOOD_ID);
        if (foodId == 0 || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no food ID");
        if(foodIds.get(foodId) == null) throw new Exception("Food eaten with ID: " + id + " references unknown food ID: " + foodId);

        String date = rs.getString(FOODS_EATEN.DATE);
        if(date == null || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no date");

        double servingQty = rs.getDouble(FOODS_EATEN.SERVING_QTY);
        if(servingQty == 0 || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no serving qty");

        Food.ServingType servingType = Food.ServingType.fromString(rs.getString(FOODS_EATEN.SERVING_TYPE));
        if(servingType == null || rs.wasNull()) throw new Exception("Malformed food eaten with ID: " + id + ", no serving type");

        return new FoodEaten(
                uuid,
                foodIds.get(foodId),
                dateFormatter.parse(date),
                servingType,
                servingQty
        );
    }

    private ExercisePerformed readExercisePerformed(ResultSet rs) throws Exception {
        int id = rs.getInt(EXERCISES_PERFORMED.ID);
        if (id == 0 || rs.wasNull()) throw new Exception("Malformed exercise performed, no ID");
        UUID uuid = UUID.randomUUID();

        // Exercise ID
        String legacyName = rs.getString(EXERCISES.NAME);
        if(legacyName == null || rs.wasNull()) throw new Exception("Malformed exercise performed with ID: " + id + ", found no exercise name");
        legacyName = legacyName.replace("\u00A0", " ").trim();  // For some reason, String.trim() doesn't remove ASCII character 160
        UUID exerciseId = exerciseIds.get(legacyName);
        if(exerciseId == null) {
            throw new Exception("Malformed exercise performed with ID: " + id + ", found no new id matching legacy name: [" + legacyName + "]");
        }

        // Date
        String date = rs.getString(EXERCISES_PERFORMED.DATE);
        if(date == null || rs.wasNull()) throw new Exception("Malformed exercise performed with ID: " + id + ", no date");

        // Minutes
        int minutes = rs.getInt(EXERCISES_PERFORMED.MINUTES);
        if(minutes == 0 || rs.wasNull()) throw new Exception("Malformed exercise performed with ID: " + id + ", no minutes");

        return new ExercisePerformed(
                uuid,
                exerciseId,
                dateFormatter.parse(date),
                minutes
        );
    }

}

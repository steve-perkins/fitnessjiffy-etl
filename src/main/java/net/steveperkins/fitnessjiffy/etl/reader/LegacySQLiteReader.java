package net.steveperkins.fitnessjiffy.etl.reader;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.steveperkins.fitnessjiffy.etl.crypto.BCrypt;
import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.model.Exercise;
import net.steveperkins.fitnessjiffy.etl.model.ExercisePerformed;
import net.steveperkins.fitnessjiffy.etl.model.Food;
import net.steveperkins.fitnessjiffy.etl.model.FoodEaten;
import net.steveperkins.fitnessjiffy.etl.model.User;
import net.steveperkins.fitnessjiffy.etl.model.Weight;

import javax.annotation.Nonnull;
import java.io.InputStream;
import java.security.SecureRandom;
import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public final class LegacySQLiteReader extends JDBCReader {

    public interface TABLES extends JDBCReader.TABLES {
        String USER = "USERS";
        String FOOD = "FOODS";
        String FOOD_EATEN = "FOODS_EATEN";
        String EXERCISE = "EXERCISES";
        String EXERCISE_PERFORMED = "EXERCISES_PERFORMED";
    }

    public interface USER extends JDBCReader.USER {
        String BIRTHDATE = null;
        String EMAIL = null;
        String PASSWORD_HASH = null;
        String CREATED_TIME = null;
        String LAST_UPDATED_TIME = null;
        String AGE = "AGE";
        String USERNAME = "USERNAME";
        String PASSWORD = "PASSWORD";
        String IS_ACTIVE = "ACTIVE";
    }

    public interface WEIGHT {
        String ID = "ID";
        String USER_ID = "USER_ID";
        String DATE = "DATE";
        String POUNDS = "POUNDS";
    }

    public interface FOOD extends JDBCReader.FOOD {
        String USER_ID = "USER_ID";
        String CREATED_TIME = null;
        String LAST_UPDATED_TIME = null;
    }

    public interface FOOD_EATEN extends JDBCReader.FOOD_EATEN {
    }

    public interface EXERCISE {
        String ID = "ID";
        String NAME = "NAME";
        String CALORIES_PER_HOUR = "CALORIES_PER_HOUR";
        String HIDDEN = "HIDDEN";
    }

    public interface EXERCISE_PERFORMED extends JDBCReader.EXERCISE_PERFORMED {
    }

    private final Map<Integer, UUID> foodIds = new HashMap<>();
    private final Map<String, UUID> exerciseIds = new HashMap<String, UUID>() {{
        put("Yoga, Hatha (32 years - 265 lbs)", UUID.fromString("6cf6d7de-abe9-4981-bea4-bb45c8d8088e"));  // code: 02150
        put("yoga, Hatha", UUID.fromString("6cf6d7de-abe9-4981-bea4-bb45c8d8088e"));
        put("Elliptical machine (14 cal/min)", UUID.fromString("fa0faf6f-90d7-446f-b8ba-a27f5bc80e72"));  // 02048
        put("Elliptical trainer, moderate effort", UUID.fromString("fa0faf6f-90d7-446f-b8ba-a27f5bc80e72"));
        put("Bowling (32 years - 265 lbs)", UUID.fromString("543d3c3e-a1c2-4a6e-a31c-e6e0e12baa1f"));  // 15092
        put("bowling, indoor, bowling alley", UUID.fromString("543d3c3e-a1c2-4a6e-a31c-e6e0e12baa1f"));
        put("Racquetball (33 years - 250 lbs)", UUID.fromString("08d08760-8504-419d-9c36-e58119cc0387"));  // 15530
        put("racquetball, general (Taylor Code 470)", UUID.fromString("08d08760-8504-419d-9c36-e58119cc0387"));
        put("Swimming (32 years - 265 lbs)", UUID.fromString("8eca5fb3-16ae-4fc0-8899-893436c2f629"));  // 18310
        put("swimming, leisurely, not lap swimming, general", UUID.fromString("8eca5fb3-16ae-4fc0-8899-893436c2f629"));
        put("Basketball, shooting baskets (33 years - 250 lbs)", UUID.fromString("9a0de32d-ee1f-4370-848b-ef7c08379b03"));  // 15070
        put("basketball, shooting baskets", UUID.fromString("9a0de32d-ee1f-4370-848b-ef7c08379b03"));
        put("Weight lifting (33 years - 250 lbs)", UUID.fromString("7b4e54b2-a28e-4555-8945-3e7e13f9b659"));  // 02050
        put("resistance training (weight lifting, free weight, nautilus or universal), power lifting or body building, vigorous effort (Taylor Code 210)",
                UUID.fromString("7b4e54b2-a28e-4555-8945-3e7e13f9b659"));
        put("Walking, 3 mph (33 years - 250 lbs)", UUID.fromString("e3a850cc-a432-4709-bf0b-a06b7fd78c8e"));  // 17190
        put("walking, 2.8 to 3.2 mph, level, moderate pace, firm surface",
                UUID.fromString("e3a850cc-a432-4709-bf0b-a06b7fd78c8e"));
        put("Elliptical machine (13 cal/min)", UUID.fromString("fa0faf6f-90d7-446f-b8ba-a27f5bc80e72"));  // 02048
        put("Elliptical trainer, moderate effort", UUID.fromString("fa0faf6f-90d7-446f-b8ba-a27f5bc80e72"));
        put("Yoga, Hatha (33 years - 250 lbs)", UUID.fromString("6cf6d7de-abe9-4981-bea4-bb45c8d8088e"));  // 02150
        put("yoga, Hatha", UUID.fromString("6cf6d7de-abe9-4981-bea4-bb45c8d8088e"));
        put("Bowling (33 years - 250 lbs)", UUID.fromString("543d3c3e-a1c2-4a6e-a31c-e6e0e12baa1f"));  // 15092
        put("bowling, indoor, bowling alley", UUID.fromString("543d3c3e-a1c2-4a6e-a31c-e6e0e12baa1f"));
        put("Stair climber (33 years - 240 lbs)", UUID.fromString("9f0f45a2-d3bd-4776-b62e-8e1cce0f0daa"));  // 02065
        put("stair-treadmill ergometer, general", UUID.fromString("9f0f45a2-d3bd-4776-b62e-8e1cce0f0daa"));
        put("Golf driving range (33 years - 240 lbs)", UUID.fromString("848bf262-da21-48a8-96f3-53a1d9a0ef9e"));  // 15270
        put("golf, miniature, driving range", UUID.fromString("848bf262-da21-48a8-96f3-53a1d9a0ef9e"));
        put("Mowing lawn, reel mower (33 years - 240 lbs)", UUID.fromString("41a7437c-a8ae-45d3-85d7-314c5b911bdf"));  // 08110
        put("mowing lawn, walk, hand mower (Taylor Code 570)", UUID.fromString("41a7437c-a8ae-45d3-85d7-314c5b911bdf"));
        put("Yard work (33 years - 240 lbs)", UUID.fromString("8e32c657-c7ba-4e8a-b44b-bf5eef49dddc"));  // 08261
        put("yard work, general, moderate effort", UUID.fromString("8e32c657-c7ba-4e8a-b44b-bf5eef49dddc"));
        put("Bicycling, 10 mph (34 years - 280 lbs)", UUID.fromString("31893938-9ef6-47f0-a362-0813940dbcf9"));  // 01020
        put("bicycling, 10-11.9 mph, leisure, slow, light effort", UUID.fromString("31893938-9ef6-47f0-a362-0813940dbcf9"));
    }};
    private final DateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");

    public LegacySQLiteReader(@Nonnull final Connection connection) {
        super(connection);
    }

    @Override
    @Nonnull
    public Datastore read() throws Exception {
        if (connection.isClosed()) {
            throw new IllegalStateException();
        }

        final Datastore datastore = new Datastore();

        // Load exercises
        try (InputStream exerciseJsonStream = this.getClass().getResourceAsStream(EXERCISES_JSON_PATH)) {
            final Set<Exercise> exercises = new ObjectMapper().readValue(exerciseJsonStream, new TypeReference<Set<Exercise>>() {
            });
            for (final Exercise exercise : exercises) {
                exercise.setDescription(exercise.getDescription().trim());
                datastore.addExercise(exercise);
            }
        }

        // Load global foods
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.FOOD + " WHERE " + FOOD.USER_ID + " IS NULL");
             ResultSet rs = statement.executeQuery()) {
            while (rs.next()) {
                datastore.addGlobalFood(readFood(rs));
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

    @Override
    @Nonnull
    protected User readUser(@Nonnull final ResultSet rs, @Nonnull final Connection connection) throws Exception {
        final int id = rs.getInt(USER.ID);
        if (id == 0 || rs.wasNull()) {
            throw new Exception("Malformed user, no ID");
        }
        final UUID uuid = UUID.randomUUID();

        final User.Gender gender = User.Gender.fromString(rs.getString(USER.GENDER));
        if (gender == null || rs.wasNull()) {
            throw new Exception("Malformed user with ID: " + id + ", no gender");
        }

        final int age = rs.getInt(USER.AGE);
        if (age == 0 || rs.wasNull()) {
            throw new Exception("Malformed user with ID: " + id + ", no age");
        }
        final Calendar currentDateMinusAge = new GregorianCalendar();
        currentDateMinusAge.set(Calendar.MONTH, Calendar.JANUARY);
        currentDateMinusAge.set(Calendar.DAY_OF_MONTH, 1);
        currentDateMinusAge.add(Calendar.YEAR, (age * -1));
        final Date birthdate = new Date(currentDateMinusAge.getTimeInMillis());

        final double heightInInches = rs.getDouble(USER.HEIGHT_IN_INCHES);
        if (heightInInches == 0 || rs.wasNull()) {
            throw new Exception("Malformed user with ID: " + id + ", no height");
        }

        final User.ActivityLevel activityLevel = User.ActivityLevel.fromValue(rs.getDouble(USER.ACTIVITY_LEVEL));
        if (activityLevel == null || rs.wasNull()) {
            throw new Exception("Malformed user with ID: " + id + ", no activity level");
        }

        final String username = rs.getString(USER.USERNAME);
        if (username == null || rs.wasNull()) {
            throw new Exception("Malformed user with ID: " + id + ", no username");
        }

        final String password = rs.getString(USER.PASSWORD);
        if (password == null || rs.wasNull()) {
            throw new Exception("Malformed user with ID: " + id + ", no password");
        }

        // Encrypt the raw password, with BCrypt code borrowed from the Spring Security project
        final String salt = BCrypt.gensalt(10, new SecureRandom());
        final String passwordHash = BCrypt.hashpw(password, salt);

        final String firstName = rs.getString(USER.FIRST_NAME);
        if (firstName == null || rs.wasNull()) {
            throw new Exception("Malformed user with ID: " + id + ", no first name");
        }

        final String lastName = rs.getString(USER.LAST_NAME);
        if (lastName == null || rs.wasNull()) {
            throw new Exception("Malformed user with ID: " + id + ", no last name");
        }

        final String isActive = rs.getString(USER.IS_ACTIVE);
        if (isActive == null || (!isActive.trim().equalsIgnoreCase("Y") && !isActive.trim().equalsIgnoreCase("N"))) {
            throw new Exception("Malformed user with ID: " + id + ", no active flag");
        }

        // Weights
        final Set<Weight> weights = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.WEIGHT + " WHERE " + WEIGHT.USER_ID + " = ?")) {
            statement.setInt(1, id);
            try (ResultSet weightsResultSet = statement.executeQuery()) {
                while (weightsResultSet.next()) {
                    weights.add(readWeight(weightsResultSet));
                }
            }
        }

        // User-owned foods
        final Set<Food> foods = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.FOOD + " WHERE " + FOOD.USER_ID + " = ?")) {
            statement.setInt(1, id);
            try (ResultSet userFoodResultSet = statement.executeQuery()) {
                while (userFoodResultSet.next()) {
                    foods.add(readFood(userFoodResultSet));
                }
            }
        }

        // Foods eaten
        final Set<FoodEaten> foodsEaten = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.FOOD_EATEN + " WHERE " + FOOD_EATEN.USER_ID + " = ?")) {
            statement.setInt(1, id);
            try (ResultSet foodsEatenResultSet = statement.executeQuery()) {
                while (foodsEatenResultSet.next()) {
                    foodsEaten.add(readFoodEaten(foodsEatenResultSet));
                }
            }
        }

        // Exercises performed
        final Set<ExercisePerformed> exercisesPerformed = new HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT " + TABLES.EXERCISE_PERFORMED + ".*, " + TABLES.EXERCISE + "." + EXERCISE.NAME
                        + " FROM " + TABLES.EXERCISE_PERFORMED + ", " + TABLES.EXERCISE
                        + " WHERE " + TABLES.EXERCISE_PERFORMED + "." + EXERCISE_PERFORMED.EXERCISE_ID + " = " + TABLES.EXERCISE + "." + EXERCISE.ID
                        + " AND " + TABLES.EXERCISE_PERFORMED + "." + EXERCISE_PERFORMED.USER_ID + " = ?")) {
            statement.setInt(1, id);
            try (ResultSet exercisesPerformedResultSet = statement.executeQuery()) {
                while (exercisesPerformedResultSet.next()) {
                    exercisesPerformed.add(readExercisePerformed(exercisesPerformedResultSet));
                }
            }
        }

        // Earliest activity (i.e. for created_date and last_updated_date)
        Timestamp earliestActivity = null;
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT MIN(DATE) FROM ("
                        + "SELECT MIN(" + WEIGHT.DATE + ") AS DATE FROM " + TABLES.WEIGHT + " WHERE " + WEIGHT.USER_ID + " = ?"
                        + " UNION SELECT MIN(" + FOOD_EATEN.DATE + ") AS DATE FROM " + TABLES.FOOD_EATEN + " WHERE " + FOOD_EATEN.USER_ID + " = ?"
                        + " UNION SELECT MIN(" + EXERCISE_PERFORMED.DATE + ") AS DATE FROM " + TABLES.EXERCISE_PERFORMED + " WHERE " + EXERCISE_PERFORMED.USER_ID + " = ?"
                        + ")"
        )) {
            statement.setInt(1, id);
            statement.setInt(2, id);
            statement.setInt(3, id);
            try (ResultSet earliestActivityResultSet = statement.executeQuery()) {
                if (earliestActivityResultSet.next()) {
                    final String earliestDateString = earliestActivityResultSet.getString(1);
                    final long earliestDate = dateFormatter.parse(earliestDateString).getTime();
                    earliestActivity = new Timestamp(earliestDate);
                }
            }
            if (earliestActivity == null) {
                earliestActivity = new Timestamp(new java.util.Date().getTime());
            }
        }

        return new User(
                uuid,
                gender,
                birthdate,
                heightInInches,
                activityLevel,
                username, // email
                passwordHash,
                firstName,
                lastName,
                earliestActivity,
                earliestActivity,
                weights,
                foods,
                foodsEaten,
                exercisesPerformed
        );
    }

    @Nonnull
    private Weight readWeight(@Nonnull final ResultSet rs) throws Exception {
        final int id = rs.getInt(WEIGHT.ID);
        if (id == 0 || rs.wasNull()) {
            throw new Exception("Malformed weight, no ID");
        }
        final UUID uuid = UUID.randomUUID();

        final String date = rs.getString(WEIGHT.DATE);
        if (date == null || rs.wasNull()) {
            throw new Exception("Malformed weight with ID: " + id + ", no date");
        }

        final Double pounds = rs.getDouble(WEIGHT.POUNDS);
        if (pounds == null || rs.wasNull()) {
            throw new Exception("Malformed weight with ID: " + id + ", no pounds");
        }

        return new Weight(
                uuid,
                new Date(dateFormatter.parse(date).getTime()),
                pounds
        );
    }

    @Nonnull
    private Food readFood(@Nonnull final ResultSet rs) throws Exception {
        final int id = rs.getInt(FOOD.ID);
        if (id == 0 || rs.wasNull()) {
            throw new Exception("Malformed food, no ID");
        }
        final UUID uuid = UUID.randomUUID();
        foodIds.put(id, uuid);

        final String name = rs.getString(FOOD.NAME);
        if (name == null || rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", no name");
        }

        final Food.ServingType defaultServingType = Food.ServingType.fromString(rs.getString(FOOD.DEFAULT_SERVING_TYPE));
        if (defaultServingType == null || rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", no default serving type");
        }

        final double servingTypeQty = rs.getDouble(FOOD.SERVING_TYPE_QTY);
        if (servingTypeQty == 0 || rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", no serving type qty");
        }

        //
        // The remaining attributes of a Food might actually be zero, so we don't treat
        // it as an error condition.
        //

        final int calories = rs.getInt(FOOD.CALORIES);
        if (rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null calories");
        }

        final double fat = rs.getDouble(FOOD.FAT);
        if (rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null fat");
        }

        final double saturatedFat = rs.getDouble(FOOD.SATURATED_FAT);
        if (rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null saturated fat");
        }

        final double carbs = rs.getDouble(FOOD.CARBS);
        if (rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null carbs");
        }

        final double fiber = rs.getDouble(FOOD.FIBER);
        if (rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null fiber");
        }

        final double sugar = rs.getDouble(FOOD.SUGAR);
        if (rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null sugar");
        }

        final double protein = rs.getDouble(FOOD.PROTEIN);
        if (rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null protein");
        }

        final double sodium = rs.getDouble(FOOD.SODIUM);
        if (rs.wasNull()) {
            throw new Exception("Malformed food with ID: " + id + ", null sodium");
        }

        Timestamp earliestActivity = null;
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT MIN(" + FOOD_EATEN.DATE + ") AS DATE FROM " + TABLES.FOOD_EATEN + " WHERE " + FOOD_EATEN.FOOD_ID + " = ?"
        )) {
            statement.setInt(1, id);
            try (ResultSet earliestActivityResultSet = statement.executeQuery()) {
                if (earliestActivityResultSet.next()) {
                    try {
                        final long earliestDate = dateFormatter.parse(earliestActivityResultSet.getString(1)).getTime();
                        earliestActivity = new Timestamp(earliestDate);
                    } catch (Exception e) {
                        earliestActivity = new Timestamp(new java.util.Date().getTime());
                    }
                } else {
                    earliestActivity = new Timestamp(new java.util.Date().getTime());
                }
            }
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
                sodium,
                earliestActivity,
                earliestActivity
        );
    }

    @Nonnull
    private FoodEaten readFoodEaten(@Nonnull final ResultSet rs) throws Exception {
        final int id = rs.getInt(FOOD_EATEN.ID);
        if (id == 0 || rs.wasNull()) {
            throw new Exception("Malformed food eaten, no ID");
        }
        final UUID uuid = UUID.randomUUID();

        final int foodId = rs.getInt(FOOD_EATEN.FOOD_ID);
        if (foodId == 0 || rs.wasNull()) {
            throw new Exception("Malformed food eaten with ID: " + id + ", no food ID");
        }
        if (foodIds.get(foodId) == null) {
            throw new Exception("Food eaten with ID: " + id + " references unknown food ID: " + foodId);
        }

        final String date = rs.getString(FOOD_EATEN.DATE);
        if (date == null || rs.wasNull()) {
            throw new Exception("Malformed food eaten with ID: " + id + ", no date");
        }

        final double servingQty = rs.getDouble(FOOD_EATEN.SERVING_QTY);
        if (servingQty == 0 || rs.wasNull()) {
            throw new Exception("Malformed food eaten with ID: " + id + ", no serving qty");
        }

        final Food.ServingType servingType = Food.ServingType.fromString(rs.getString(FOOD_EATEN.SERVING_TYPE));
        if (servingType == null || rs.wasNull()) {
            throw new Exception("Malformed food eaten with ID: " + id + ", no serving type");
        }

        return new FoodEaten(
                uuid,
                foodIds.get(foodId),
                new Date(dateFormatter.parse(date).getTime()),
                servingType,
                servingQty
        );
    }

    @Nonnull
    private ExercisePerformed readExercisePerformed(@Nonnull final ResultSet rs) throws Exception {
        final int id = rs.getInt(EXERCISE_PERFORMED.ID);
        if (id == 0 || rs.wasNull()) {
            throw new Exception("Malformed exercise performed, no ID");
        }
        final UUID uuid = UUID.randomUUID();

        // Exercise ID
        String legacyName = rs.getString(EXERCISE.NAME);
        if (legacyName == null || rs.wasNull()) {
            throw new Exception("Malformed exercise performed with ID: " + id + ", found no exercise name");
        }
        legacyName = legacyName.replace("\u00A0", " ").trim();  // For some reason, String.trim() doesn't remove ASCII character 160
        final UUID exerciseId = exerciseIds.get(legacyName);
        if (exerciseId == null) {
            throw new Exception("Malformed exercise performed with ID: " + id + ", found no new id matching legacy name: [" + legacyName + "]");
        }

        // Date
        final String date = rs.getString(EXERCISE_PERFORMED.DATE);
        if (date == null || rs.wasNull()) {
            throw new Exception("Malformed exercise performed with ID: " + id + ", no date");
        }

        // Minutes
        final int minutes = rs.getInt(EXERCISE_PERFORMED.MINUTES);
        if (minutes == 0 || rs.wasNull()) {
            throw new Exception("Malformed exercise performed with ID: " + id + ", no minutes");
        }

        return new ExercisePerformed(
                uuid,
                exerciseId,
                new Date(dateFormatter.parse(date).getTime()),
                minutes
        );
    }

}

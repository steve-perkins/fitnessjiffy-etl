package net.steveperkins.fitnessjiffy.etl.reader;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.model.Exercise;
import net.steveperkins.fitnessjiffy.etl.model.ExercisePerformed;
import net.steveperkins.fitnessjiffy.etl.model.Food;
import net.steveperkins.fitnessjiffy.etl.model.FoodEaten;
import net.steveperkins.fitnessjiffy.etl.model.ReportData;
import net.steveperkins.fitnessjiffy.etl.model.User;
import net.steveperkins.fitnessjiffy.etl.model.Weight;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

public abstract class JDBCReader {

    public interface TABLES {
        String USER = "FITNESSJIFFY_USER";
        String WEIGHT = "WEIGHT";
        String FOOD = "FOOD";
        String FOOD_EATEN = "FOOD_EATEN";
        String EXERCISE = "EXERCISE";
        String EXERCISE_PERFORMED = "EXERCISE_PERFORMED";
        String REPORT_DATA = "REPORT_DATA";
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

    public interface REPORT_DATA {
        String ID = "ID";
        String USER_ID = "USER_ID";
        String DATE = "DATE";
        String POUNDS = "POUNDS";
        String NET_CALORIES = "NET_CALORIES";
        String NET_POINTS = "NET_POINTS";
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
        try (
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.EXERCISE);
                ResultSet rs = statement.executeQuery();
        ) {
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
        try (
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.FOOD + " WHERE " + FOOD.USER_ID + " IS NULL");
                ResultSet rs = statement.executeQuery();
        ) {
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
        try (
                PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + TABLES.USER);
                ResultSet rs = statement.executeQuery();
        ) {
            while (rs.next()) {
                datastore.addUser(readUser(rs, connection));
            }
        }

        // Load (or generate) report data for each user
        for (final User user : datastore.getUsers()) {
            final Set<ReportData> reportData = new HashSet<>();
            try (
                    PreparedStatement statement = connection.prepareStatement(
                            "SELECT * FROM " + TABLES.REPORT_DATA + " WHERE " + TABLES.REPORT_DATA + "." + REPORT_DATA.USER_ID + " = ?"
                    )
            ) {
                statement.setObject(1, user.getId(), Types.BINARY);
                try (ResultSet rs = statement.executeQuery()) {
                    while (rs.next()) {
                        final ReportData reportDataRow = new ReportData(
                                UUID.nameUUIDFromBytes(rs.getBytes(REPORT_DATA.ID)),
                                rs.getDate(REPORT_DATA.DATE),
                                rs.getDouble(REPORT_DATA.POUNDS),
                                rs.getInt(REPORT_DATA.NET_CALORIES),
                                rs.getDouble(REPORT_DATA.NET_POINTS)
                        );
                        reportData.add(reportDataRow);
                    }
                }
                if (reportData.isEmpty()) {
                    throw new Exception();
                }
            } catch (Exception e) {
                // Some database models (e.g. Legacy SQLite, or early versions of H2 and PostgreSQL) did not have the REPORT_DATA
                // table.  So if an exception is thrown when tryin to access this table, fall back to generating the data instead.
                final Set<ReportData> generatedReportData = generateReportData(
                        user,
                        datastore.getGlobalFoods(),
                        datastore.getExercises()
                );
                reportData.clear();
                reportData.addAll(generatedReportData);
            }
            user.setReportData(reportData);
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
                "SELECT * FROM " + TABLES.EXERCISE_PERFORMED + " WHERE " + TABLES.EXERCISE_PERFORMED + "." + EXERCISE_PERFORMED.USER_ID + " = ?"
        )) {
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

    protected Set<ReportData> generateReportData(
            @Nonnull final User user,
            @Nonnull final Set<Food> globalFoods,
            @Nonnull final Set<Exercise> exercises
    ) {
        //
        // Populate lookup maps for this user's data, while detecting the date range all this data encompasses.
        //

        final Map<Date, Weight> weightsByDate = new TreeMap<>();
        final Map<Date, Set<FoodEaten>> foodsEatenByDate = new TreeMap<>();
        final Map<Date, Set<ExercisePerformed>> exercisesPerformedByDate = new TreeMap<>();
        final Map<UUID, Food> foodsById = new TreeMap<>();
        final Map<UUID, Exercise> exercisesById = new TreeMap<>();

        Date earliestDateForUser = null;
        Date latestDateForUser = null;

        for (final Weight weight : user.getWeights()) {
            if (earliestDateForUser == null || weight.getDate().compareTo(earliestDateForUser) < 0) {
                earliestDateForUser = weight.getDate();
            }
            if (latestDateForUser == null || weight.getDate().compareTo(latestDateForUser) > 0) {
                latestDateForUser = weight.getDate();
            }
            weightsByDate.put(weight.getDate(), weight);
        }

        for (final FoodEaten foodEaten : user.getFoodsEaten()) {
            if (earliestDateForUser == null || foodEaten.getDate().compareTo(earliestDateForUser) < 0) {
                earliestDateForUser = foodEaten.getDate();
            }
            if (latestDateForUser == null || foodEaten.getDate().compareTo(latestDateForUser) > 0) {
                latestDateForUser = foodEaten.getDate();
            }
            final Set<FoodEaten> foodsEatenThisDate =
                    foodsEatenByDate.containsKey(foodEaten.getDate()) ? foodsEatenByDate.get(foodEaten.getDate()) : new HashSet<FoodEaten>();
            foodsEatenThisDate.add(foodEaten);
            foodsEatenByDate.put(foodEaten.getDate(), foodsEatenThisDate);
        }

        for (final ExercisePerformed exercisePerformed : user.getExercisesPerformed()) {
            if (earliestDateForUser == null || exercisePerformed.getDate().compareTo(earliestDateForUser) < 0) {
                earliestDateForUser = exercisePerformed.getDate();
            }
            if (latestDateForUser == null || exercisePerformed.getDate().compareTo(latestDateForUser) > 0) {
                latestDateForUser = exercisePerformed.getDate();
            }
            final Set<ExercisePerformed> exercisesPerformedThisDate =
                    exercisesPerformedByDate.containsKey(exercisePerformed.getDate()) ? exercisesPerformedByDate.get(exercisePerformed.getDate()) : new HashSet<ExercisePerformed>();
            exercisesPerformedThisDate.add(exercisePerformed);
            exercisesPerformedByDate.put(exercisePerformed.getDate(), exercisesPerformedThisDate);
        }

        for (final Food food : globalFoods) {
            foodsById.put(food.getId(), food);
        }
        for (final Food food : user.getFoods()) {
            foodsById.put(food.getId(), food);
        }

        for (final Exercise exercise : exercises) {
            exercisesById.put(exercise.getId(), exercise);
        }

        //
        // Iterate through each day in the date range
        //

        final Set<ReportData> reportData = new HashSet<>();
        Weight mostRecentWeight = null;
        Date currentDate = earliestDateForUser;
        while (currentDate.compareTo(latestDateForUser) <= 0) {

            // Determine weight on this day
            Weight weight = weightsByDate.get(currentDate);
            if (weight == null && mostRecentWeight != null) {
                weight = mostRecentWeight;
            } else if (weight == null) {
                final Date date = new Date(
                        new LocalDate(DateTimeZone.UTC).toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis()
                );
                weight = new Weight(UUID.randomUUID(), date, 0.0);
            }
            mostRecentWeight = weight;

            // Iterate through the foods eaten performed on this day, tallying net calories and points
            int netCalories = 0;
            double netPoints = 0.0;
            final Set<FoodEaten> foodsEaten = foodsEatenByDate.get(currentDate) == null ? new HashSet<FoodEaten>() : foodsEatenByDate.get(currentDate);
            for (final FoodEaten foodEaten : foodsEaten) {
                final Food food = foodsById.get(foodEaten.getFoodId());

                // Calculate default points for this food
                final double fiber = (food.getFiber() <= 4) ? food.getFiber() : 4.0;
                double points = (food.getCalories() / 50.0) + (food.getFat() / 12.0) - (fiber / 5.0);
                points = (points > 0) ? points : 0.0;

                // Calculate the ratio between this food's default serving size and the serving actually eaten
                double ratio;
                if (foodEaten.getServingType().equals(food.getDefaultServingType())) {
                    // Default serving type was used
                    ratio = foodEaten.getServingQty() / food.getServingTypeQty();
                } else {
                    // Serving type needs conversion
                    final double ouncesInThisServingType = foodEaten.getServingType().getValue();
                    final double ouncesInDefaultServingType = food.getDefaultServingType().getValue();
                    ratio = (ouncesInDefaultServingType * food.getServingTypeQty() == 0) ? 0 : (ouncesInThisServingType * foodEaten.getServingQty()) / (ouncesInDefaultServingType * food.getServingTypeQty());
                }

                netCalories += (food.getCalories() * ratio);
                netPoints += (points * ratio);
            }

            // Iterate through the exercises performed on this day, adjusting the net calories and points
            final Set<ExercisePerformed> exercisesPerformed = exercisesPerformedByDate.get(currentDate) == null ? new HashSet<ExercisePerformed>() : exercisesPerformedByDate.get(currentDate);
            for (final ExercisePerformed exercisePerformed : exercisesPerformed) {
                final Exercise exercise = exercisesById.get(exercisePerformed.getExerciseId());

                // Calories burned
                final double weightInKilograms = weight.getPounds() / 2.2;
                final int caloriesBurned = (int) (exercise.getMetabolicEquivalent() * 3.5 * weightInKilograms / 200 * exercisePerformed.getMinutes());

                // Points burned
                final int caloriesBurnedPerHour = (int) (exercise.getMetabolicEquivalent() * 3.5 * weightInKilograms / 200 * 60);
                double pointsBurned;
                if (caloriesBurnedPerHour < 400) {
                    pointsBurned = weight.getPounds() * exercisePerformed.getMinutes() * 0.000232;
                } else if (caloriesBurnedPerHour < 900) {
                    pointsBurned = weight.getPounds() * exercisePerformed.getMinutes() * 0.000327;
                } else {
                    pointsBurned = weight.getPounds() * exercisePerformed.getMinutes() * 0.0008077;
                }

                netCalories -= caloriesBurned;
                netPoints -= pointsBurned;
            }

            //
            // Add a record for this date, and then increment to the next day
            //

            final ReportData reportDataRow = new ReportData(
                    UUID.randomUUID(),
                    currentDate,
                    weight.getPounds(),
                    netCalories,
                    netPoints
            );
            reportData.add(reportDataRow);

            final LocalDate today = new LocalDate(currentDate.getTime(), DateTimeZone.UTC);
            final LocalDate tommorrow = today.plusDays(1);
            final DateTime startOfTommorrow = tommorrow.toDateTimeAtStartOfDay(DateTimeZone.UTC);
            currentDate = new Date(startOfTommorrow.getMillis());
        }

        return reportData;
    }

}

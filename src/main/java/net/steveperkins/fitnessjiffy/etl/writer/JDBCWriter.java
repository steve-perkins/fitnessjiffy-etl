package net.steveperkins.fitnessjiffy.etl.writer;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.model.Exercise;
import net.steveperkins.fitnessjiffy.etl.model.ExercisePerformed;
import net.steveperkins.fitnessjiffy.etl.model.Food;
import net.steveperkins.fitnessjiffy.etl.model.FoodEaten;
import net.steveperkins.fitnessjiffy.etl.model.ReportData;
import net.steveperkins.fitnessjiffy.etl.model.User;
import net.steveperkins.fitnessjiffy.etl.model.Weight;
import net.steveperkins.fitnessjiffy.etl.reader.JDBCReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.UUID;

public abstract class JDBCWriter {

    protected Connection connection;
    protected Datastore datastore;

    public JDBCWriter(
            @Nonnull final Connection connection,
            @Nonnull final Datastore datastore
    ) {
        if (connection == null || datastore == null) {
            throw new NullPointerException();
        }
        this.connection = connection;
        this.datastore = datastore;
    }

    public void write() throws Exception {
        if (connection.isClosed()) {
            throw new IllegalStateException();
        }
        connection.setAutoCommit(false);

        writeSchema();
        writeExercises();
        writeGlobalFoods();
        writeUsers();

        connection.commit();
    }

    protected abstract void writeSchema() throws SQLException;

    protected void writeExercises() throws SQLException {
        for (final Exercise exercise : datastore.getExercises()) {
            final String sql = "INSERT INTO " + JDBCReader.TABLES.EXERCISE + " (" + JDBCReader.EXERCISE.ID + ", " + JDBCReader.EXERCISE.CATEGORY + ", "
                    + JDBCReader.EXERCISE.CODE + ", " + JDBCReader.EXERCISE.DESCRIPTION + ", " + JDBCReader.EXERCISE.METABOLIC_EQUIVALENT
                    + ") VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, exercise.getId(), Types.BINARY);
                statement.setString(2, exercise.getCategory());
                statement.setString(3, exercise.getCode());
                statement.setString(4, exercise.getDescription());
                statement.setDouble(5, exercise.getMetabolicEquivalent());
                statement.executeUpdate();
            }
        }
    }

    protected void writeGlobalFoods() throws SQLException {
        for (final Food food : datastore.getGlobalFoods()) {
            writeFood(food, null);
        }
    }

    protected void writeUsers() throws Exception {
        for (final User user : datastore.getUsers()) {
            final String userSql = "INSERT INTO " + JDBCReader.TABLES.USER + " (" + JDBCReader.USER.ID + ", " + JDBCReader.USER.GENDER + ", " + JDBCReader.USER.BIRTHDATE + ", " + JDBCReader.USER.HEIGHT_IN_INCHES
                    + ", " + JDBCReader.USER.ACTIVITY_LEVEL + ", " + JDBCReader.USER.EMAIL + ", " + JDBCReader.USER.PASSWORD_HASH + ", " + JDBCReader.USER.FIRST_NAME
                    + ", " + JDBCReader.USER.LAST_NAME + ", " + JDBCReader.USER.CREATED_TIME + ", " + JDBCReader.USER.LAST_UPDATED_TIME + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(userSql)) {
                statement.setObject(1, user.getId(), Types.BINARY);
                statement.setString(2, user.getGender().toString());
                statement.setDate(3, user.getBirthdate());
                statement.setDouble(4, user.getHeightInInches());
                statement.setDouble(5, user.getActivityLevel().getValue());
                statement.setString(6, user.getEmail());
                statement.setString(7, user.getPasswordHash());
                statement.setString(8, user.getFirstName());
                statement.setString(9, user.getLastName());
                statement.setTimestamp(10, user.getCreatedTime());
                statement.setTimestamp(11, user.getLastUpdatedTime());
                statement.executeUpdate();
            }

            for (final Weight weight : user.getWeights()) {
                final String sql = "INSERT INTO " + JDBCReader.TABLES.WEIGHT + " (" + JDBCReader.WEIGHT.ID + ", " + JDBCReader.WEIGHT.USER_ID + ", " + JDBCReader.WEIGHT.DATE + ", "
                        + JDBCReader.WEIGHT.POUNDS + ") VALUES (?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, weight.getId(), Types.BINARY);
                    statement.setObject(2, user.getId(), Types.BINARY);
                    statement.setDate(3, weight.getDate());
                    statement.setDouble(4, weight.getPounds());
                    statement.executeUpdate();
                }
            }

            for (final Food food : user.getFoods()) {
                writeFood(food, user.getId());
            }

            for (final FoodEaten foodEaten : user.getFoodsEaten()) {
                final String sql = "INSERT INTO " + JDBCReader.TABLES.FOOD_EATEN + " (" + JDBCReader.FOOD_EATEN.ID + ", " + JDBCReader.FOOD_EATEN.USER_ID + ", "
                        + JDBCReader.FOOD_EATEN.FOOD_ID + ", " + JDBCReader.FOOD_EATEN.DATE + ", " + JDBCReader.FOOD_EATEN.SERVING_TYPE + ", "
                        + JDBCReader.FOOD_EATEN.SERVING_QTY + ") VALUES (?, ?, ?, ?, ? ,?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, foodEaten.getId(), Types.BINARY);
                    statement.setObject(2, user.getId(), Types.BINARY);
                    statement.setObject(3, foodEaten.getFoodId(), Types.BINARY);
                    statement.setDate(4, foodEaten.getDate());
                    statement.setString(5, foodEaten.getServingType().toString());
                    statement.setDouble(6, foodEaten.getServingQty());
                    statement.executeUpdate();
                }
            }

            for (final ExercisePerformed exercisePerformed : user.getExercisesPerformed()) {
                final String sql = "INSERT INTO " + JDBCReader.TABLES.EXERCISE_PERFORMED + " (" + JDBCReader.EXERCISE_PERFORMED.ID + ", "
                        + JDBCReader.EXERCISE_PERFORMED.USER_ID + ", " + JDBCReader.EXERCISE_PERFORMED.EXERCISE_ID + ", "
                        + JDBCReader.EXERCISE_PERFORMED.DATE + ", " + JDBCReader.EXERCISE_PERFORMED.MINUTES + ") VALUES (?, ?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, exercisePerformed.getId(), Types.BINARY);
                    statement.setObject(2, user.getId(), Types.BINARY);
                    statement.setObject(3, exercisePerformed.getExerciseId(), Types.BINARY);
                    statement.setDate(4, exercisePerformed.getDate());
                    statement.setInt(5, exercisePerformed.getMinutes());
                    statement.executeUpdate();
                }
            }

            for (final ReportData reportData : user.getReportData()) {
                final String sql = "INSERT INTO " + JDBCReader.TABLES.REPORT_DATA + "(" + JDBCReader.REPORT_DATA.ID + ", "
                        + JDBCReader.REPORT_DATA.USER_ID + ", " + JDBCReader.REPORT_DATA.DATE + ", " + JDBCReader.REPORT_DATA.POUNDS + ", "
                        + JDBCReader.REPORT_DATA.NET_CALORIES + ", " + JDBCReader.REPORT_DATA.NET_POINTS + ") VALUES (?, ?, ?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, reportData.getId(), Types.BINARY);
                    statement.setObject(2, user.getId(), Types.BINARY);
                    statement.setDate(3, reportData.getDate());
                    statement.setDouble(4, reportData.getPounds());
                    statement.setInt(5, reportData.getNetCalories());
                    statement.setDouble(6, reportData.getNetPoints());
                    statement.executeUpdate();
                }
            }
        }
    }

    protected void writeFood(
            @Nonnull final Food food,
            @Nullable final UUID ownerId
    ) throws SQLException {
        String sql = "INSERT INTO " + JDBCReader.TABLES.FOOD + " (" + JDBCReader.FOOD.ID + ", " + JDBCReader.FOOD.NAME + ", " + JDBCReader.FOOD.DEFAULT_SERVING_TYPE + ", "
                + JDBCReader.FOOD.SERVING_TYPE_QTY + ", " + JDBCReader.FOOD.CALORIES + ", " + JDBCReader.FOOD.FAT + ", " + JDBCReader.FOOD.SATURATED_FAT + ", "
                + JDBCReader.FOOD.CARBS + ", " + JDBCReader.FOOD.FIBER + ", " + JDBCReader.FOOD.SUGAR + ", " + JDBCReader.FOOD.PROTEIN + ", " + JDBCReader.FOOD.SODIUM + ", "
                + JDBCReader.FOOD.CREATED_TIME + ", " + JDBCReader.FOOD.LAST_UPDATED_TIME;
        sql += (ownerId != null)
                ? ", " + JDBCReader.FOOD.USER_ID + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                : ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, food.getId(), Types.BINARY);
            statement.setString(2, food.getName());
            statement.setString(3, food.getDefaultServingType().toString());
            statement.setFloat(4, food.getServingTypeQty().floatValue());
            statement.setInt(5, food.getCalories());
            statement.setFloat(6, food.getFat().floatValue());
            statement.setFloat(7, food.getSaturatedFat().floatValue());
            statement.setFloat(8, food.getCarbs().floatValue());
            statement.setFloat(9, food.getFiber().floatValue());
            statement.setFloat(10, food.getSugar().floatValue());
            statement.setFloat(11, food.getProtein().floatValue());
            statement.setFloat(12, food.getSodium().floatValue());
            statement.setTimestamp(13, food.getCreatedTime());
            statement.setTimestamp(14, food.getLastUpdatedTime());
            if (ownerId != null) {
                statement.setObject(15, ownerId, Types.BINARY);
            }
            statement.executeUpdate();
        }
    }

}

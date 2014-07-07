package net.steveperkins.fitnessjiffy.etl.writer;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.model.Exercise;
import net.steveperkins.fitnessjiffy.etl.model.ExercisePerformed;
import net.steveperkins.fitnessjiffy.etl.model.Food;
import net.steveperkins.fitnessjiffy.etl.model.FoodEaten;
import net.steveperkins.fitnessjiffy.etl.model.User;
import net.steveperkins.fitnessjiffy.etl.model.Weight;
import net.steveperkins.fitnessjiffy.etl.reader.JDBCReader;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public final class PostgresWriter extends JDBCWriter {

    public PostgresWriter(
            @Nonnull final Connection connection,
            @Nonnull final Datastore datastore
    ) {
        super(connection, datastore);
    }

    @Override
    protected void writeSchema() throws SQLException {
        final String ddl = "SET statement_timeout = 0;\n" +
                "SET lock_timeout = 0;\n" +
                "SET client_encoding = 'UTF8';\n" +
                "SET standard_conforming_strings = on;\n" +
                "SET check_function_bodies = false;\n" +
                "SET client_min_messages = warning;\n" +
                "CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;\n" +
                "COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';\n" +
                "SET search_path = public, pg_catalog;\n" +
                "SET default_tablespace = '';\n" +
                "SET default_with_oids = false;\n" +
                "\n" +
                "CREATE TABLE IF NOT EXISTS exercise (\n" +
                "    id bytea NOT NULL,\n" +
                "    category character varying(25) NOT NULL,\n" +
                "    code character varying(5) NOT NULL,\n" +
                "    description character varying(250) NOT NULL,\n" +
                "    metabolic_equivalent double precision NOT NULL\n" +
                ");\n" +
                "-- ALTER TABLE public.exercise OWNER TO postgres;\n" +
                "CREATE TABLE IF NOT EXISTS exercise_performed (\n" +
                "    id bytea NOT NULL,\n" +
                "    date date NOT NULL,\n" +
                "    minutes integer NOT NULL,\n" +
                "    exercise_id bytea NOT NULL,\n" +
                "    user_id bytea NOT NULL\n" +
                ");\n" +
                "-- ALTER TABLE public.exercise_performed OWNER TO postgres;\n" +
                "CREATE TABLE IF NOT EXISTS fitnessjiffy_user (\n" +
                "    id bytea NOT NULL,\n" +
                "    activity_level double precision NOT NULL,\n" +
                "    birthdate date NOT NULL,\n" +
                "    first_name character varying(20) NOT NULL,\n" +
                "    gender character varying(6) NOT NULL,\n" +
                "    height_in_inches double precision NOT NULL,\n" +
                "    last_name character varying(20) NOT NULL,\n" +
                "    password_hash character varying(100),\n" +
                "    email character varying(100) NOT NULL,\n" +
                "    created_time timestamp NOT NULL,\n" +
                "    last_updated_time timestamp NOT NULL\n" +
                ");\n" +
                "-- ALTER TABLE public.fitnessjiffy_user OWNER TO postgres;\n" +
                "CREATE TABLE IF NOT EXISTS food (\n" +
                "    id bytea NOT NULL,\n" +
                "    calories integer NOT NULL,\n" +
                "    carbs double precision NOT NULL,\n" +
                "    default_serving_type character varying(10) NOT NULL,\n" +
                "    fat double precision NOT NULL,\n" +
                "    fiber double precision NOT NULL,\n" +
                "    name character varying(50) NOT NULL,\n" +
                "    protein double precision NOT NULL,\n" +
                "    saturated_fat double precision NOT NULL,\n" +
                "    serving_type_qty double precision NOT NULL,\n" +
                "    sodium double precision NOT NULL,\n" +
                "    sugar double precision NOT NULL,\n" +
                "    owner_id bytea,\n" +
                "    created_time timestamp NOT NULL,\n" +
                "    last_updated_time timestamp NOT NULL\n" +
                ");\n" +
                "-- ALTER TABLE public.food OWNER TO postgres;\n" +
                "CREATE TABLE IF NOT EXISTS food_eaten (\n" +
                "    id bytea NOT NULL,\n" +
                "    date date NOT NULL,\n" +
                "    serving_qty double precision NOT NULL,\n" +
                "    serving_type character varying(10) NOT NULL,\n" +
                "    food_id bytea NOT NULL,\n" +
                "    user_id bytea NOT NULL\n" +
                ");\n" +
                "-- ALTER TABLE public.food_eaten OWNER TO postgres;\n" +
                "CREATE TABLE IF NOT EXISTS weight (\n" +
                "    id bytea NOT NULL,\n" +
                "    date date NOT NULL,\n" +
                "    pounds double precision NOT NULL,\n" +
                "    user_id bytea NOT NULL\n" +
                ");\n" +
                "-- ALTER TABLE public.weight OWNER TO postgres;\n" +
                "\n" +
                "ALTER TABLE ONLY exercise_performed DROP CONSTRAINT IF EXISTS exercise_performed_pkey CASCADE;\n" +
                "ALTER TABLE ONLY exercise \t\t\tDROP CONSTRAINT IF EXISTS exercise_pkey CASCADE;\n" +
                "ALTER TABLE ONLY fitnessjiffy_user \tDROP CONSTRAINT IF EXISTS fitnessjiffy_user_pkey CASCADE;\n" +
                "ALTER TABLE ONLY food_eaten \t\tDROP CONSTRAINT IF EXISTS food_eaten_pkey CASCADE;\n" +
                "ALTER TABLE ONLY food \t\t\t\tDROP CONSTRAINT IF EXISTS food_pkey CASCADE;\n" +
                "ALTER TABLE ONLY weight \t\t\tDROP CONSTRAINT IF EXISTS weight_pkey CASCADE;\n" +
                "ALTER TABLE ONLY exercise_performed DROP CONSTRAINT IF EXISTS fk_52nub55r5musrfyjsvpth76bh CASCADE;\n" +
                "ALTER TABLE ONLY food_eaten \t\tDROP CONSTRAINT IF EXISTS fk_a6t0pikjip5a2k9jntw8s0755 CASCADE;\n" +
                "ALTER TABLE ONLY food_eaten\t\t\tDROP CONSTRAINT IF EXISTS fk_fqyglhvonkjbp4kd7htfy02cb CASCADE;\n" +
                "ALTER TABLE ONLY food \t\t\t\tDROP CONSTRAINT IF EXISTS fk_k8ugf925yeo9p3f8vwdo8ctsu CASCADE;\n" +
                "ALTER TABLE ONLY exercise_performed DROP CONSTRAINT IF EXISTS fk_o3b6rrwboc2sshggrq8hjw3xu CASCADE;\n" +
                "ALTER TABLE ONLY weight \t\t\tDROP CONSTRAINT IF EXISTS fk_rus9mpsdmijsl6fujhhud5pgu CASCADE;\n" +
                "\n" +
                "ALTER TABLE ONLY exercise_performed ADD CONSTRAINT exercise_performed_pkey PRIMARY KEY (id);\n" +
                "ALTER TABLE ONLY exercise\t \t\tADD CONSTRAINT exercise_pkey PRIMARY KEY (id);\n" +
                "ALTER TABLE ONLY fitnessjiffy_user\tADD CONSTRAINT fitnessjiffy_user_pkey PRIMARY KEY (id);\n" +
                "ALTER TABLE ONLY food_eaten\t\t\tADD CONSTRAINT food_eaten_pkey PRIMARY KEY (id);\n" +
                "ALTER TABLE ONLY food\t\t\t\tADD CONSTRAINT food_pkey PRIMARY KEY (id);\n" +
                "ALTER TABLE ONLY weight\t\t\t\tADD CONSTRAINT weight_pkey PRIMARY KEY (id);\n" +
                "ALTER TABLE ONLY exercise_performed ADD CONSTRAINT fk_52nub55r5musrfyjsvpth76bh FOREIGN KEY (exercise_id) REFERENCES exercise(id);\n" +
                "ALTER TABLE ONLY food_eaten\t\t\tADD CONSTRAINT fk_a6t0pikjip5a2k9jntw8s0755 FOREIGN KEY (food_id) REFERENCES food(id);\n" +
                "ALTER TABLE ONLY food_eaten\t\t\tADD CONSTRAINT fk_fqyglhvonkjbp4kd7htfy02cb FOREIGN KEY (user_id) REFERENCES fitnessjiffy_user(id);\n" +
                "ALTER TABLE ONLY food\t\t\t\tADD CONSTRAINT fk_k8ugf925yeo9p3f8vwdo8ctsu FOREIGN KEY (owner_id) REFERENCES fitnessjiffy_user(id);\n" +
                "ALTER TABLE ONLY exercise_performed ADD CONSTRAINT fk_o3b6rrwboc2sshggrq8hjw3xu FOREIGN KEY (user_id) REFERENCES fitnessjiffy_user(id);\n" +
                "ALTER TABLE ONLY weight\t\t\t\tADD CONSTRAINT fk_rus9mpsdmijsl6fujhhud5pgu FOREIGN KEY (user_id) REFERENCES fitnessjiffy_user(id);\n" +
                "\n" +
                "-- REVOKE ALL ON SCHEMA public FROM PUBLIC;\n" +
                "-- REVOKE ALL ON SCHEMA public FROM postgres;\n" +
                "-- GRANT ALL ON SCHEMA public TO postgres;\n" +
                "-- GRANT ALL ON SCHEMA public TO PUBLIC;\n";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(ddl);
        }
    }

    @Override
    protected void writeExercises() throws SQLException {
        for (final Exercise exercise : datastore.getExercises()) {
            final String sql = "INSERT INTO " + JDBCReader.TABLES.EXERCISE + " (" + JDBCReader.EXERCISE.ID + ", " + JDBCReader.EXERCISE.CATEGORY + ", "
                    + JDBCReader.EXERCISE.CODE + ", " + JDBCReader.EXERCISE.DESCRIPTION + ", " + JDBCReader.EXERCISE.METABOLIC_EQUIVALENT
                    + ") VALUES (?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setBytes(1, uuidToBytes(exercise.getId()));
                statement.setString(2, exercise.getCategory());
                statement.setString(3, exercise.getCode());
                statement.setString(4, exercise.getDescription());
                statement.setDouble(5, exercise.getMetabolicEquivalent());
                statement.executeUpdate();
            }
        }
    }

    @Override
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
            statement.setBytes(1, uuidToBytes(food.getId()));
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
                statement.setBytes(15, uuidToBytes(ownerId));
            }
            statement.executeUpdate();
        }
    }

    @Override
    protected void writeUsers() throws Exception {
        for (final User user : datastore.getUsers()) {
            final String userSql = "INSERT INTO " + JDBCReader.TABLES.USER + " (" + JDBCReader.USER.ID + ", " + JDBCReader.USER.GENDER + ", " + JDBCReader.USER.BIRTHDATE + ", " + JDBCReader.USER.HEIGHT_IN_INCHES
                    + ", " + JDBCReader.USER.ACTIVITY_LEVEL + ", " + JDBCReader.USER.EMAIL + ", " + JDBCReader.USER.PASSWORD_HASH + ", " + JDBCReader.USER.FIRST_NAME
                    + ", " + JDBCReader.USER.LAST_NAME + ", " + JDBCReader.USER.CREATED_TIME + ", " + JDBCReader.USER.LAST_UPDATED_TIME + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(userSql)) {
                statement.setBytes(1, uuidToBytes(user.getId()));
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
                    statement.setBytes(1, uuidToBytes(weight.getId()));
                    statement.setBytes(2, uuidToBytes(user.getId()));
                    statement.setDate(3, new Date(weight.getDate().getTime()));
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
                    statement.setBytes(1, uuidToBytes(foodEaten.getId()));
                    statement.setBytes(2, uuidToBytes(user.getId()));
                    statement.setBytes(3, uuidToBytes(foodEaten.getFoodId()));
                    statement.setDate(4, new Date(foodEaten.getDate().getTime()));
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
                    statement.setBytes(1, uuidToBytes(exercisePerformed.getId()));
                    statement.setBytes(2, uuidToBytes(user.getId()));
                    statement.setBytes(3, uuidToBytes(exercisePerformed.getExerciseId()));
                    statement.setDate(4, new Date(exercisePerformed.getDate().getTime()));
                    statement.setInt(5, exercisePerformed.getMinutes());
                    statement.executeUpdate();
                }
            }
        }
    }


    private byte[] uuidToBytes(@Nonnull final UUID uuid) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        return byteBuffer.array();
    }

}

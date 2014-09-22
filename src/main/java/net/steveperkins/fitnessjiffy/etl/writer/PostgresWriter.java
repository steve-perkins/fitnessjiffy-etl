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
        final String ddl = "--\n" +
                "-- PostgreSQL database dump\n" +
                "--\n" +
                "\n" +
                "-- Dumped from database version 9.3.5\n" +
                "-- Dumped by pg_dump version 9.3.5\n" +
                "-- Started on 2014-09-20 12:56:46\n" +
                "\n" +
                "SET statement_timeout = 0;\n" +
                "SET lock_timeout = 0;\n" +
                "SET client_encoding = 'UTF8';\n" +
                "SET standard_conforming_strings = on;\n" +
                "SET check_function_bodies = false;\n" +
                "SET client_min_messages = warning;\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 177 (class 3079 OID 11750)\n" +
                "-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner:\n" +
                "--\n" +
                "\n" +
                "CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1997 (class 0 OID 0)\n" +
                "-- Dependencies: 177\n" +
                "-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner:\n" +
                "--\n" +
                "\n" +
                "COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';\n" +
                "\n" +
                "\n" +
                "SET search_path = public, pg_catalog;\n" +
                "\n" +
                "SET default_tablespace = '';\n" +
                "\n" +
                "SET default_with_oids = false;\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 170 (class 1259 OID 18702)\n" +
                "-- Name: exercise; Type: TABLE; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "CREATE TABLE exercise (\n" +
                "    id bytea NOT NULL,\n" +
                "    category character varying(25) NOT NULL,\n" +
                "    code character varying(5) NOT NULL,\n" +
                "    description character varying(250) NOT NULL,\n" +
                "    metabolic_equivalent double precision NOT NULL\n" +
                ");\n" +
                "\n" +
                "\n" +
                "ALTER TABLE public.exercise OWNER TO fitnessjiffy;\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 171 (class 1259 OID 18710)\n" +
                "-- Name: exercise_performed; Type: TABLE; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "CREATE TABLE exercise_performed (\n" +
                "    id bytea NOT NULL,\n" +
                "    date date NOT NULL,\n" +
                "    minutes integer NOT NULL,\n" +
                "    exercise_id bytea NOT NULL,\n" +
                "    user_id bytea NOT NULL\n" +
                ");\n" +
                "\n" +
                "\n" +
                "ALTER TABLE public.exercise_performed OWNER TO fitnessjiffy;\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 172 (class 1259 OID 18718)\n" +
                "-- Name: fitnessjiffy_user; Type: TABLE; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "CREATE TABLE fitnessjiffy_user (\n" +
                "    id bytea NOT NULL,\n" +
                "    activity_level double precision NOT NULL,\n" +
                "    birthdate date NOT NULL,\n" +
                "    created_time timestamp without time zone NOT NULL,\n" +
                "    email character varying(100) NOT NULL,\n" +
                "    first_name character varying(20) NOT NULL,\n" +
                "    gender character varying(6) NOT NULL,\n" +
                "    height_in_inches double precision NOT NULL,\n" +
                "    last_name character varying(20) NOT NULL,\n" +
                "    last_updated_time timestamp without time zone NOT NULL,\n" +
                "    password_hash character varying(100)\n" +
                ");\n" +
                "\n" +
                "\n" +
                "ALTER TABLE public.fitnessjiffy_user OWNER TO fitnessjiffy;\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 173 (class 1259 OID 18726)\n" +
                "-- Name: food; Type: TABLE; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "CREATE TABLE food (\n" +
                "    id bytea NOT NULL,\n" +
                "    calories integer NOT NULL,\n" +
                "    carbs double precision NOT NULL,\n" +
                "    created_time timestamp without time zone NOT NULL,\n" +
                "    default_serving_type character varying(10) NOT NULL,\n" +
                "    fat double precision NOT NULL,\n" +
                "    fiber double precision NOT NULL,\n" +
                "    last_updated_time timestamp without time zone NOT NULL,\n" +
                "    name character varying(50) NOT NULL,\n" +
                "    protein double precision NOT NULL,\n" +
                "    saturated_fat double precision NOT NULL,\n" +
                "    serving_type_qty double precision NOT NULL,\n" +
                "    sodium double precision NOT NULL,\n" +
                "    sugar double precision NOT NULL,\n" +
                "    owner_id bytea\n" +
                ");\n" +
                "\n" +
                "\n" +
                "ALTER TABLE public.food OWNER TO fitnessjiffy;\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 174 (class 1259 OID 18734)\n" +
                "-- Name: food_eaten; Type: TABLE; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "CREATE TABLE food_eaten (\n" +
                "    id bytea NOT NULL,\n" +
                "    date date NOT NULL,\n" +
                "    serving_qty double precision NOT NULL,\n" +
                "    serving_type character varying(10) NOT NULL,\n" +
                "    food_id bytea NOT NULL,\n" +
                "    user_id bytea NOT NULL\n" +
                ");\n" +
                "\n" +
                "\n" +
                "ALTER TABLE public.food_eaten OWNER TO fitnessjiffy;\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 175 (class 1259 OID 18742)\n" +
                "-- Name: report_data; Type: TABLE; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "CREATE TABLE report_data (\n" +
                "    id bytea NOT NULL,\n" +
                "    date date NOT NULL,\n" +
                "    net_calories integer NOT NULL,\n" +
                "    net_points double precision NOT NULL,\n" +
                "    pounds double precision NOT NULL,\n" +
                "    user_id bytea NOT NULL\n" +
                ");\n" +
                "\n" +
                "\n" +
                "ALTER TABLE public.report_data OWNER TO fitnessjiffy;\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 176 (class 1259 OID 18750)\n" +
                "-- Name: weight; Type: TABLE; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "CREATE TABLE weight (\n" +
                "    id bytea NOT NULL,\n" +
                "    date date NOT NULL,\n" +
                "    pounds double precision NOT NULL,\n" +
                "    user_id bytea NOT NULL\n" +
                ");\n" +
                "\n" +
                "\n" +
                "ALTER TABLE public.weight OWNER TO fitnessjiffy;\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1855 (class 2606 OID 18717)\n" +
                "-- Name: exercise_performed_pkey; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY exercise_performed\n" +
                "    ADD CONSTRAINT exercise_performed_pkey PRIMARY KEY (id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1853 (class 2606 OID 18709)\n" +
                "-- Name: exercise_pkey; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY exercise\n" +
                "    ADD CONSTRAINT exercise_pkey PRIMARY KEY (id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1859 (class 2606 OID 18725)\n" +
                "-- Name: fitnessjiffy_user_pkey; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY fitnessjiffy_user\n" +
                "    ADD CONSTRAINT fitnessjiffy_user_pkey PRIMARY KEY (id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1865 (class 2606 OID 18741)\n" +
                "-- Name: food_eaten_pkey; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY food_eaten\n" +
                "    ADD CONSTRAINT food_eaten_pkey PRIMARY KEY (id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1861 (class 2606 OID 18733)\n" +
                "-- Name: food_pkey; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY food\n" +
                "    ADD CONSTRAINT food_pkey PRIMARY KEY (id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1869 (class 2606 OID 18749)\n" +
                "-- Name: report_data_pkey; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY report_data\n" +
                "    ADD CONSTRAINT report_data_pkey PRIMARY KEY (id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1871 (class 2606 OID 18765)\n" +
                "-- Name: uk_5bacnypi0a0a5vcxaqovytq93; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY report_data\n" +
                "    ADD CONSTRAINT uk_5bacnypi0a0a5vcxaqovytq93 UNIQUE (user_id, date);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1867 (class 2606 OID 18763)\n" +
                "-- Name: uk_o17xkhthgnqe2icjgamjbun93; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY food_eaten\n" +
                "    ADD CONSTRAINT uk_o17xkhthgnqe2icjgamjbun93 UNIQUE (user_id, food_id, date);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1857 (class 2606 OID 18759)\n" +
                "-- Name: uk_oc1fognywyv0fn3dcogp2nn8e; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY exercise_performed\n" +
                "    ADD CONSTRAINT uk_oc1fognywyv0fn3dcogp2nn8e UNIQUE (user_id, exercise_id, date);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1863 (class 2606 OID 18761)\n" +
                "-- Name: uk_of9wdgtxdh2mgh2cfh3spllvi; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY food\n" +
                "    ADD CONSTRAINT uk_of9wdgtxdh2mgh2cfh3spllvi UNIQUE (id, owner_id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1873 (class 2606 OID 18767)\n" +
                "-- Name: uk_r4ky9e01cp3060j1hgmmqo220; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY weight\n" +
                "    ADD CONSTRAINT uk_r4ky9e01cp3060j1hgmmqo220 UNIQUE (user_id, date);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1875 (class 2606 OID 18757)\n" +
                "-- Name: weight_pkey; Type: CONSTRAINT; Schema: public; Owner: fitnessjiffy; Tablespace:\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY weight\n" +
                "    ADD CONSTRAINT weight_pkey PRIMARY KEY (id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1876 (class 2606 OID 18768)\n" +
                "-- Name: fk_52nub55r5musrfyjsvpth76bh; Type: FK CONSTRAINT; Schema: public; Owner: fitnessjiffy\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY exercise_performed\n" +
                "    ADD CONSTRAINT fk_52nub55r5musrfyjsvpth76bh FOREIGN KEY (exercise_id) REFERENCES exercise(id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1879 (class 2606 OID 18783)\n" +
                "-- Name: fk_a6t0pikjip5a2k9jntw8s0755; Type: FK CONSTRAINT; Schema: public; Owner: fitnessjiffy\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY food_eaten\n" +
                "    ADD CONSTRAINT fk_a6t0pikjip5a2k9jntw8s0755 FOREIGN KEY (food_id) REFERENCES food(id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1880 (class 2606 OID 18788)\n" +
                "-- Name: fk_fqyglhvonkjbp4kd7htfy02cb; Type: FK CONSTRAINT; Schema: public; Owner: fitnessjiffy\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY food_eaten\n" +
                "    ADD CONSTRAINT fk_fqyglhvonkjbp4kd7htfy02cb FOREIGN KEY (user_id) REFERENCES fitnessjiffy_user(id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1878 (class 2606 OID 18778)\n" +
                "-- Name: fk_k8ugf925yeo9p3f8vwdo8ctsu; Type: FK CONSTRAINT; Schema: public; Owner: fitnessjiffy\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY food\n" +
                "    ADD CONSTRAINT fk_k8ugf925yeo9p3f8vwdo8ctsu FOREIGN KEY (owner_id) REFERENCES fitnessjiffy_user(id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1881 (class 2606 OID 18793)\n" +
                "-- Name: fk_mm7j7rv35awetxl921usmtdm4; Type: FK CONSTRAINT; Schema: public; Owner: fitnessjiffy\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY report_data\n" +
                "    ADD CONSTRAINT fk_mm7j7rv35awetxl921usmtdm4 FOREIGN KEY (user_id) REFERENCES fitnessjiffy_user(id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1877 (class 2606 OID 18773)\n" +
                "-- Name: fk_o3b6rrwboc2sshggrq8hjw3xu; Type: FK CONSTRAINT; Schema: public; Owner: fitnessjiffy\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY exercise_performed\n" +
                "    ADD CONSTRAINT fk_o3b6rrwboc2sshggrq8hjw3xu FOREIGN KEY (user_id) REFERENCES fitnessjiffy_user(id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1882 (class 2606 OID 18798)\n" +
                "-- Name: fk_rus9mpsdmijsl6fujhhud5pgu; Type: FK CONSTRAINT; Schema: public; Owner: fitnessjiffy\n" +
                "--\n" +
                "\n" +
                "ALTER TABLE ONLY weight\n" +
                "    ADD CONSTRAINT fk_rus9mpsdmijsl6fujhhud5pgu FOREIGN KEY (user_id) REFERENCES fitnessjiffy_user(id);\n" +
                "\n" +
                "\n" +
                "--\n" +
                "-- TOC entry 1996 (class 0 OID 0)\n" +
                "-- Dependencies: 5\n" +
                "-- Name: public; Type: ACL; Schema: -; Owner: postgres\n" +
                "--\n" +
                "\n" +
                "REVOKE ALL ON SCHEMA public FROM PUBLIC;\n" +
                "REVOKE ALL ON SCHEMA public FROM postgres;\n" +
                "GRANT ALL ON SCHEMA public TO postgres;\n" +
                "GRANT ALL ON SCHEMA public TO PUBLIC;\n" +
                "\n" +
                "\n" +
                "-- Completed on 2014-09-20 12:56:46\n" +
                "\n" +
                "--\n" +
                "-- PostgreSQL database dump complete\n" +
                "--\n" +
                "\n";
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

            for (final ReportData reportData : user.getReportData()) {
                final String sql = "INSERT INTO " + JDBCReader.TABLES.REPORT_DATA + "(" + JDBCReader.REPORT_DATA.ID + ", "
                        + JDBCReader.REPORT_DATA.USER_ID + ", " + JDBCReader.REPORT_DATA.DATE + ", " + JDBCReader.REPORT_DATA.POUNDS + ", "
                        + JDBCReader.REPORT_DATA.NET_CALORIES + ", " + JDBCReader.REPORT_DATA.NET_POINTS + ") VALUES (?, ?, ?, ?, ?, ?)";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setBytes(1, uuidToBytes(reportData.getId()));
                    statement.setObject(2, uuidToBytes(user.getId()));
                    statement.setDate(3, reportData.getDate());
                    statement.setDouble(4, reportData.getPounds());
                    statement.setInt(5, reportData.getNetCalories());
                    statement.setDouble(6, reportData.getNetPoints());
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

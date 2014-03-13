package net.steveperkins.fitnessjiffy.etl.reader;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;

import java.sql.Connection;

public abstract class JDBCReader {

    public interface TABLES {
        public static String USER = "FITNESSJIFFY_USER";
        public static String WEIGHT = "WEIGHT";
        public static String FOOD = "FOOD";
        public static String FOOD_EATEN = "FOOD_EATEN";
        public static String EXERCISE = "EXERCISE";
        public static String EXERCISE_PERFORMED = "EXERCISE_PERFORMED";
    }
    public interface USER {
        public static final String ID = "ID";
        public static final String GENDER = "GENDER";
        public static final String AGE = "AGE";
        public static final String HEIGHT_IN_INCHES = "HEIGHT_IN_INCHES";
        public static final String ACTIVITY_LEVEL = "ACTIVITY_LEVEL";
        public static final String USERNAME = "USERNAME";
        public static final String PASSWORD = "PASSWORD";
        public static final String FIRST_NAME= "FIRST_NAME";
        public static final String LAST_NAME = "LAST_NAME";
        public static final String IS_ACTIVE = "IS_ACTIVE";
    }
    public interface WEIGHT {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String DATE = "DATE";
        public static final String POUNDS = "POUNDS";
    }
    public interface FOOD {
        public static final String ID = "ID";
        public static final String USER_ID = "OWNER_ID";
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
    public interface FOOD_EATEN {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String FOOD_ID = "FOOD_ID";
        public static final String DATE = "DATE";
        public static final String SERVING_QTY = "SERVING_QTY";
        public static final String SERVING_TYPE = "SERVING_TYPE";
    }
    public interface EXERCISE {
        public static final String ID = "ID";
        public static final String CATEGORY = "CATEGORY";
        public static final String CODE = "CODE";
        public static final String DESCRIPTION = "DESCRIPTION";
        public static final String METABOLIC_EQUIVALENT = "METABOLIC_EQUIVALENT";
    }
    public interface EXERCISE_PERFORMED {
        public static final String ID = "ID";
        public static final String USER_ID = "USER_ID";
        public static final String EXERCISE_ID = "EXERCISE_ID";
        public static final String DATE = "DATE";
        public static final String MINUTES = "MINUTES";
    }

    protected static final String EXERCISES_JSON_PATH = "/exercises.json";

    protected Connection connection;

    public JDBCReader(Connection connection) {
        if(connection == null) throw new NullPointerException();
        this.connection = connection;
    }

    public abstract Datastore read() throws Exception;  // TODO: Create custom Exception wrapper type?

}

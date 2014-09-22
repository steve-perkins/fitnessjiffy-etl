package net.steveperkins.fitnessjiffy.etl.writer;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public final class H2Writer extends JDBCWriter {

    public H2Writer(
            @Nonnull final Connection connection,
            @Nonnull final Datastore datastore
    ) {
        super(connection, datastore);
    }

    @Override
    protected void writeSchema() throws SQLException {
        final String ddl = "CREATE USER IF NOT EXISTS SA SALT '9f7587665670254d' HASH 'bb416164378221e2f16082dc7c49960430610d5935aaf96e18243b4a177a6639' ADMIN;\n" +
                "CREATE CACHED TABLE PUBLIC.EXERCISE(\n" +
                "    ID BYTEA NOT NULL,\n" +
                "    CATEGORY VARCHAR(25) NOT NULL,\n" +
                "    CODE VARCHAR(5) NOT NULL,\n" +
                "    DESCRIPTION VARCHAR(250) NOT NULL,\n" +
                "    METABOLIC_EQUIVALENT FLOAT8 NOT NULL\n" +
                ");\n" +
                "ALTER TABLE PUBLIC.EXERCISE ADD CONSTRAINT PUBLIC.CONSTRAINT_A PRIMARY KEY(ID);\n" +
                "     \n" +
                "CREATE CACHED TABLE PUBLIC.EXERCISE_PERFORMED(\n" +
                "    ID BYTEA NOT NULL,\n" +
                "    DATE DATE NOT NULL,\n" +
                "    MINUTES INT4 NOT NULL,\n" +
                "    EXERCISE_ID BYTEA NOT NULL,\n" +
                "    USER_ID BYTEA NOT NULL\n" +
                ");\n" +
                "ALTER TABLE PUBLIC.EXERCISE_PERFORMED ADD CONSTRAINT PUBLIC.CONSTRAINT_7 PRIMARY KEY(ID);\n" +
                "\n" +
                "CREATE CACHED TABLE PUBLIC.FOOD(\n" +
                "    ID BYTEA NOT NULL,\n" +
                "    CALORIES INT4 NOT NULL,\n" +
                "    CARBS FLOAT8 NOT NULL,\n" +
                "    CREATED_TIME TIMESTAMP NOT NULL,\n" +
                "    DEFAULT_SERVING_TYPE VARCHAR(10) NOT NULL,\n" +
                "    FAT FLOAT8 NOT NULL,\n" +
                "    FIBER FLOAT8 NOT NULL,\n" +
                "    LAST_UPDATED_TIME TIMESTAMP NOT NULL,\n" +
                "    NAME VARCHAR(50) NOT NULL,\n" +
                "    PROTEIN FLOAT8 NOT NULL,\n" +
                "    SATURATED_FAT FLOAT8 NOT NULL,\n" +
                "    SERVING_TYPE_QTY FLOAT8 NOT NULL,\n" +
                "    SODIUM FLOAT8 NOT NULL,\n" +
                "    SUGAR FLOAT8 NOT NULL,\n" +
                "    OWNER_ID BYTEA\n" +
                ");\n" +
                "ALTER TABLE PUBLIC.FOOD ADD CONSTRAINT PUBLIC.CONSTRAINT_2 PRIMARY KEY(ID);\n" +
                "        \n" +
                "CREATE CACHED TABLE PUBLIC.FOOD_EATEN(\n" +
                "    ID BYTEA NOT NULL,\n" +
                "    DATE DATE NOT NULL,\n" +
                "    SERVING_QTY FLOAT8 NOT NULL,\n" +
                "    SERVING_TYPE VARCHAR(10) NOT NULL,\n" +
                "    FOOD_ID BYTEA NOT NULL,\n" +
                "    USER_ID BYTEA NOT NULL\n" +
                ");\n" +
                "            \n" +
                "CREATE CACHED TABLE PUBLIC.FITNESSJIFFY_USER(\n" +
                "    ID BYTEA NOT NULL,\n" +
                "    ACTIVITY_LEVEL FLOAT8 NOT NULL,\n" +
                "    BIRTHDATE DATE NOT NULL,\n" +
                "    CREATED_TIME TIMESTAMP NOT NULL,\n" +
                "    EMAIL VARCHAR(100) NOT NULL,\n" +
                "    FIRST_NAME VARCHAR(20) NOT NULL,\n" +
                "    GENDER VARCHAR(6) NOT NULL,\n" +
                "    HEIGHT_IN_INCHES FLOAT8 NOT NULL,\n" +
                "    LAST_NAME VARCHAR(20) NOT NULL,\n" +
                "    LAST_UPDATED_TIME TIMESTAMP NOT NULL,\n" +
                "    PASSWORD_HASH VARCHAR(100)\n" +
                ");\n" +
                "ALTER TABLE PUBLIC.FITNESSJIFFY_USER ADD CONSTRAINT PUBLIC.CONSTRAINT_1 PRIMARY KEY(ID);\n" +
                "\n" +
                "CREATE CACHED TABLE PUBLIC.WEIGHT(\n" +
                "    ID BYTEA NOT NULL,\n" +
                "    DATE DATE NOT NULL,\n" +
                "    POUNDS FLOAT8 NOT NULL,\n" +
                "    USER_ID BYTEA NOT NULL\n" +
                ");\n" +
                "ALTER TABLE PUBLIC.WEIGHT ADD CONSTRAINT PUBLIC.CONSTRAINT_9 PRIMARY KEY(ID);\n" +
                "\n" +
                "CREATE CACHED TABLE PUBLIC.REPORT_DATA(\n" +
                "    ID BYTEA NOT NULL,\n" +
                "    DATE DATE NOT NULL,\n" +
                "    NET_CALORIES INT4 NOT NULL,\n" +
                "    NET_POINTS FLOAT8 NOT NULL,\n" +
                "    POUNDS FLOAT8 NOT NULL,\n" +
                "    USER_ID BYTEA NOT NULL\n" +
                ");\n" +
                "ALTER TABLE PUBLIC.REPORT_DATA ADD CONSTRAINT PUBLIC.CONSTRAINT_73 PRIMARY KEY(ID);\n" +
                "\n" +
                "ALTER TABLE PUBLIC.FOOD_EATEN ADD CONSTRAINT PUBLIC.UK_O17XKHTHGNQE2ICJGAMJBUN93 UNIQUE(USER_ID, FOOD_ID, DATE);\n" +
                "ALTER TABLE PUBLIC.REPORT_DATA ADD CONSTRAINT PUBLIC.UK_5BACNYPI0A0A5VCXAQOVYTQ93 UNIQUE(USER_ID, DATE);\n" +
                "ALTER TABLE PUBLIC.FOOD ADD CONSTRAINT PUBLIC.UK_OF9WDGTXDH2MGH2CFH3SPLLVI UNIQUE(ID, OWNER_ID);\n" +
                "ALTER TABLE PUBLIC.WEIGHT ADD CONSTRAINT PUBLIC.UK_R4KY9E01CP3060J1HGMMQO220 UNIQUE(USER_ID, DATE);\n" +
                "ALTER TABLE PUBLIC.EXERCISE_PERFORMED ADD CONSTRAINT PUBLIC.UK_OC1FOGNYWYV0FN3DCOGP2NN8E UNIQUE(USER_ID, EXERCISE_ID, DATE);\n" +
                "ALTER TABLE PUBLIC.EXERCISE_PERFORMED ADD CONSTRAINT PUBLIC.FK_O3B6RRWBOC2SSHGGRQ8HJW3XU FOREIGN KEY(USER_ID) REFERENCES PUBLIC.FITNESSJIFFY_USER(ID) NOCHECK;\n" +
                "ALTER TABLE PUBLIC.WEIGHT ADD CONSTRAINT PUBLIC.FK_RUS9MPSDMIJSL6FUJHHUD5PGU FOREIGN KEY(USER_ID) REFERENCES PUBLIC.FITNESSJIFFY_USER(ID) NOCHECK;\n" +
                "ALTER TABLE PUBLIC.REPORT_DATA ADD CONSTRAINT PUBLIC.FK_MM7J7RV35AWETXL921USMTDM4 FOREIGN KEY(USER_ID) REFERENCES PUBLIC.FITNESSJIFFY_USER(ID) NOCHECK;\n" +
                "ALTER TABLE PUBLIC.FOOD ADD CONSTRAINT PUBLIC.FK_K8UGF925YEO9P3F8VWDO8CTSU FOREIGN KEY(OWNER_ID) REFERENCES PUBLIC.FITNESSJIFFY_USER(ID) NOCHECK;\n" +
                "ALTER TABLE PUBLIC.EXERCISE_PERFORMED ADD CONSTRAINT PUBLIC.FK_52NUB55R5MUSRFYJSVPTH76BH FOREIGN KEY(EXERCISE_ID) REFERENCES PUBLIC.EXERCISE(ID) NOCHECK;\n" +
                "ALTER TABLE PUBLIC.FOOD_EATEN ADD CONSTRAINT PUBLIC.FK_A6T0PIKJIP5A2K9JNTW8S0755 FOREIGN KEY(FOOD_ID) REFERENCES PUBLIC.FOOD(ID) NOCHECK;\n" +
                "ALTER TABLE PUBLIC.FOOD_EATEN ADD CONSTRAINT PUBLIC.FK_FQYGLHVONKJBP4KD7HTFY02CB FOREIGN KEY(USER_ID) REFERENCES PUBLIC.FITNESSJIFFY_USER(ID) NOCHECK;\n";
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(ddl);
        }
    }

}

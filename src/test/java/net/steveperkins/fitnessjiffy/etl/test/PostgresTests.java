package net.steveperkins.fitnessjiffy.etl.test;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.reader.H2Reader;
import net.steveperkins.fitnessjiffy.etl.reader.PostgresReader;
import net.steveperkins.fitnessjiffy.etl.writer.PostgresWriter;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Date;

import static junit.framework.TestCase.assertEquals;

public class PostgresTests extends AbstractTests {

    private static final String JDBC_BASE_URL = "jdbc:postgresql://localhost/";
    private static final String JDBC_USERNAME = "postgres";
    private static final String JDBC_PASSWORD = "postgres";

    @Before
    public void before() throws Exception {
        Class.forName("org.postgresql.Driver");
        cleanFileInWorkingDirectory("output.json");
    }

    @Test
    public void roundTripTest() throws Exception {
        // Read test data from the test H2 database
        Connection readConnection = DriverManager.getConnection("jdbc:h2:" + CURRENT_WORKING_DIRECTORY + "h2");
        Datastore datastore = new H2Reader(readConnection).read();
        readConnection.close();

        // Create a temporary new PostgreSQL database
        String tempDbName = "fitnessjiffy_" + new Date().getTime();
        Connection templateConnection = DriverManager.getConnection(JDBC_BASE_URL + "template1", JDBC_USERNAME, JDBC_PASSWORD);
        Statement templateStatement = templateConnection.createStatement();
        templateStatement.execute("CREATE DATABASE " + tempDbName + " WITH ENCODING = 'UTF8'");
        templateStatement.close();
        templateConnection.close();

        // Write the test data to the new PostgreSQL database
        Connection writeConnection = DriverManager.getConnection(JDBC_BASE_URL + tempDbName, JDBC_USERNAME, JDBC_PASSWORD);
        new PostgresWriter(writeConnection, datastore).write();
        writeConnection.close();

        // Do a round-trip read of the new database, and confirm its etl has the same expected size
        Connection confirmationConnection = DriverManager.getConnection(JDBC_BASE_URL + tempDbName, JDBC_USERNAME, JDBC_PASSWORD);
        Datastore newDatastore = new PostgresReader(confirmationConnection).read();
        confirmationConnection.close();

        // Drop the temporary PostgreSQL database
        templateConnection = DriverManager.getConnection(JDBC_BASE_URL + "template1", JDBC_USERNAME, JDBC_PASSWORD);
        templateStatement = templateConnection.createStatement();
        templateStatement.execute("DROP DATABASE " + tempDbName);
        templateStatement.close();
        templateConnection.close();

        // Check results
        assertEquals(newDatastore.getExercises().size(), datastore.getExercises().size());
        assertEquals(newDatastore.getGlobalFoods().size(), datastore.getGlobalFoods().size());
        assertEquals(newDatastore.getUsers().size(), datastore.getUsers().size());
        assertEquals(newDatastore.getUsers().iterator().next().getWeights().size(), datastore.getUsers().iterator().next().getWeights().size());
        assertEquals(newDatastore.getUsers().iterator().next().getFoods().size(), datastore.getUsers().iterator().next().getFoods().size());
        assertEquals(newDatastore.getUsers().iterator().next().getFoodsEaten().size(), datastore.getUsers().iterator().next().getFoodsEaten().size());
        assertEquals(newDatastore.getUsers().iterator().next().getExercisesPerformed().size(), datastore.getUsers().iterator().next().getExercisesPerformed().size());
    }


}

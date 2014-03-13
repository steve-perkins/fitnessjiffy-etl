package net.steveperkins.fitnessjiffy.etl.test;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.reader.LegacySQLiteReader;
import net.steveperkins.fitnessjiffy.etl.writer.LegacySQLiteWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import static junit.framework.TestCase.assertEquals;

public class LegacySQLiteTests extends AbstractTests {

    protected static final int EXPECTED_JSON_STRING_LENGTH = 3472663;
    protected static final int EXPECTED_JSON_FILE_LENGTH = 3472810;

    @Before
    public void before() throws Exception {
        Class.forName("org.sqlite.JDBC");
        cleanFileInWorkingDirectory("sqlite-temp.db");
        cleanFileInWorkingDirectory("h2-temp.h2.db");
    }

    @Test
    public void canReadTest() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "sqlite.db");

        // Test conversion to JSON string
        String jsonString = new LegacySQLiteReader(connection).read().toJSONString();
        assertEquals(EXPECTED_JSON_STRING_LENGTH, jsonString.length());

        // Test output to JSON file
        File jsonFile = new File(CURRENT_WORKING_DIRECTORY + "output.json");
        new LegacySQLiteReader(connection).read().toJSONFile(jsonFile);
        assertEquals(EXPECTED_JSON_FILE_LENGTH, jsonFile.length());

        connection.close();
    }

    @Test
    public void canWriteTest() throws Exception {
        // Read the existing database
        Connection readConnection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "sqlite.db");
        Datastore datastore = new LegacySQLiteReader(readConnection).read();
        readConnection.close();

        // Write its contents to a new database
        Connection writeConnection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "sqlite-temp.db");
        new LegacySQLiteWriter(writeConnection, datastore).write();
        writeConnection.close();

        // Do a round-trip read of the new database, and confirm its etl has the same expected size
        Connection confirmationConnection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "sqlite-temp.db");
        Datastore newDatastore = new LegacySQLiteReader(confirmationConnection).read();
        confirmationConnection.close();
        assertEquals(newDatastore.getExercises().size(), datastore.getExercises().size());
        assertEquals(newDatastore.getGlobalFoods().size(), datastore.getGlobalFoods().size());
        assertEquals(newDatastore.getUsers().size(), datastore.getUsers().size());
        assertEquals(newDatastore.getUsers().iterator().next().getWeights().size(), datastore.getUsers().iterator().next().getWeights().size());
        assertEquals(newDatastore.getUsers().iterator().next().getFoods().size(), datastore.getUsers().iterator().next().getFoods().size());
        assertEquals(newDatastore.getUsers().iterator().next().getFoodsEaten().size(), datastore.getUsers().iterator().next().getFoodsEaten().size());
        assertEquals(newDatastore.getUsers().iterator().next().getExercisesPerformed().size(), datastore.getUsers().iterator().next().getExercisesPerformed().size());
    }

}

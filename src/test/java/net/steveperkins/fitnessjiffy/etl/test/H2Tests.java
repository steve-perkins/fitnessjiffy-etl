package net.steveperkins.fitnessjiffy.etl.test;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.reader.H2Reader;
import net.steveperkins.fitnessjiffy.etl.writer.H2Writer;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import static junit.framework.TestCase.assertEquals;

public final class H2Tests extends AbstractTests {

    @Before
    public void before() throws Exception {
        Class.forName("org.sqlite.JDBC");
        cleanFileInWorkingDirectory("sqlite-temp.h2.db");
        cleanFileInWorkingDirectory("h2-temp.h2.db");
        cleanFileInWorkingDirectory("output.json");
    }

    @Test
    public void canReadTest() throws Exception {
        final Connection connection = DriverManager.getConnection("jdbc:h2:" + CURRENT_WORKING_DIRECTORY + "h2");

        // Test conversion to JSON string
        final String jsonString = new H2Reader(connection).read().toJSONString();
        assertEquals(EXPECTED_JSON_STRING_LENGTH, jsonString.length());

        // Test output to JSON file
        final File jsonFile = new File(CURRENT_WORKING_DIRECTORY + "output.json");
        new H2Reader(connection).read().toJSONFile(jsonFile);
        assertEquals(EXPECTED_JSON_FILE_LENGTH, jsonFile.length());

        connection.close();
    }

    @Test
    public void canWriteTest() throws Exception {
        // Read the existing database
        final Connection readConnection = DriverManager.getConnection("jdbc:h2:" + CURRENT_WORKING_DIRECTORY + "h2");
        final Datastore datastore = new H2Reader(readConnection).read();
        readConnection.close();

        // Write its contents to a new database
        final Connection writeConnection = DriverManager.getConnection("jdbc:h2:" + CURRENT_WORKING_DIRECTORY + "h2-temp");
        new H2Writer(writeConnection, datastore).write();
        writeConnection.close();

        // Do a round-trip read of the new database, and confirm its etl has the same expected size
        final Connection confirmationConnection = DriverManager.getConnection("jdbc:h2:" + CURRENT_WORKING_DIRECTORY + "h2-temp");
        final Datastore newDatastore = new H2Reader(confirmationConnection).read();
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

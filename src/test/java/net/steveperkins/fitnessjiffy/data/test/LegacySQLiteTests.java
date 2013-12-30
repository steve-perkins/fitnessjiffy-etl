package net.steveperkins.fitnessjiffy.data.test;

import net.steveperkins.fitnessjiffy.data.model.Datastore;
import net.steveperkins.fitnessjiffy.data.model.Exercise;
import net.steveperkins.fitnessjiffy.data.reader.LegacySQLiteReader;
import net.steveperkins.fitnessjiffy.data.writer.LegacySQLiteWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;

public class LegacySQLiteTests {

    private final String CURRENT_WORKING_DIRECTORY = this.getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
    private final int EXPECTED_JSON_STRING_LENGTH = 3472661;
    private final int EXPECTED_JSON_FILE_LENGTH = 3472808;

    @Before
    public void before() throws Exception {
        Class.forName("org.sqlite.JDBC");
        File tempSqliteFile = new File(CURRENT_WORKING_DIRECTORY + "temp.db");
        if(tempSqliteFile.exists()) {
            if(!tempSqliteFile.delete()) {
                throw new Exception("There is an existing file " + tempSqliteFile.getCanonicalPath()
                        + " which can't be deleted for some reason.  Please delete this file manually.");
            }
        }
    }

    @Test
    public void canReadTest() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "sqlite.db");

        // Test conversion to JSON string
        String jsonString = new LegacySQLiteReader(connection).read().toJSONString();
        assertEquals(jsonString.length(), EXPECTED_JSON_STRING_LENGTH);

        // Test output to JSON file
        File jsonFile = new File(CURRENT_WORKING_DIRECTORY + "output.json");
        if(jsonFile.exists()) jsonFile.delete();
        new LegacySQLiteReader(connection).read().toJSONFile(jsonFile);
        assertEquals(jsonFile.length(), EXPECTED_JSON_FILE_LENGTH);

        connection.close();
    }

    @Test
    public void canWriteTest() throws Exception {
        // Read the existing database
        Connection readConnection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "sqlite.db");
        Datastore datastore = new LegacySQLiteReader(readConnection).read();
        readConnection.close();

        // Write its contents to a new database
        Connection writeConnection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "temp.db");
        new LegacySQLiteWriter(writeConnection, datastore).write();
        writeConnection.close();

        Set<Exercise> cleanExercises = new HashSet<>(datastore.getExercises());

        // Do a round-trip read of the new database, and confirm its data has the same expected size
        Connection confirmationConnection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "temp.db");
        Datastore newDatastore = new LegacySQLiteReader(confirmationConnection).read();
        confirmationConnection.close();
        assertEquals(datastore.getExercises().size(), newDatastore.getExercises().size());
        assertEquals(datastore.getGlobalFoods().size(), newDatastore.getGlobalFoods().size());
        assertEquals(datastore.getUsers().size(), newDatastore.getUsers().size());
        assertEquals(datastore.getUsers().iterator().next().getWeights().size(), newDatastore.getUsers().iterator().next().getWeights().size());
        assertEquals(datastore.getUsers().iterator().next().getFoods().size(), newDatastore.getUsers().iterator().next().getFoods().size());
        assertEquals(datastore.getUsers().iterator().next().getFoodsEaten().size(), newDatastore.getUsers().iterator().next().getFoodsEaten().size());
        assertEquals(datastore.getUsers().iterator().next().getExercisesPerformed().size(), newDatastore.getUsers().iterator().next().getExercisesPerformed().size());
    }

}

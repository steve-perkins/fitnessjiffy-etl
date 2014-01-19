package net.steveperkins.fitnessjiffy.etl.test;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.reader.LegacySQLiteReader;
import net.steveperkins.fitnessjiffy.etl.writer.H2Writer;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

public class MigrationTests {

    private final String CURRENT_WORKING_DIRECTORY = this.getClass().getProtectionDomain().getCodeSource().getLocation().getFile();

    @Before
    public void before() throws Exception {
        Class.forName("org.sqlite.JDBC");
        Class.forName("org.h2.Driver");
        cleanFileInWorkingDirectory("sqlite-temp.h2.db");
        cleanFileInWorkingDirectory("h2-temp.h2.db");
        cleanFileInWorkingDirectory("output.json");
    }

    private void cleanFileInWorkingDirectory(String name) throws Exception {
        File theFile = new File(CURRENT_WORKING_DIRECTORY + name);
        if(theFile.exists()) {
            if(!theFile.delete()) {
                throw new Exception("There is an existing file " + theFile.getCanonicalPath()
                        + " which can't be deleted for some reason.  Please delete this file manually.");
            }
        }
    }

    @Test
    public void canMigrateLegacySQLiteToH2Test() throws Exception {
        Connection legacySQLiteConnection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "sqlite.db");
        Datastore datastore = new LegacySQLiteReader(legacySQLiteConnection).read();
        legacySQLiteConnection.close();

        Connection h2Connection = DriverManager.getConnection("jdbc:h2:" + CURRENT_WORKING_DIRECTORY + "h2-temp");
        new H2Writer(h2Connection, datastore).write();
        h2Connection.close();
    }

}

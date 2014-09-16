package net.steveperkins.fitnessjiffy.etl.test;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.reader.LegacySQLiteReader;
import net.steveperkins.fitnessjiffy.etl.writer.H2Writer;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;

public final class MigrationTest extends AbstractTest {

    @Before
    public void before() throws Exception {
        Class.forName("org.sqlite.JDBC");
        Class.forName("org.h2.Driver");
        cleanFileInWorkingDirectory("sqlite-temp.h2.db");
        cleanFileInWorkingDirectory("h2-temp.h2.db");
        cleanFileInWorkingDirectory("output.json");
    }

    @Test
    public void canMigrateLegacySQLiteToH2Test() throws Exception {
        final Connection legacySQLiteConnection = DriverManager.getConnection("jdbc:sqlite:" + CURRENT_WORKING_DIRECTORY + "sqlite.db");
        final Datastore datastore = new LegacySQLiteReader(legacySQLiteConnection).read();
        legacySQLiteConnection.close();

        final Connection h2Connection = DriverManager.getConnection("jdbc:h2:" + CURRENT_WORKING_DIRECTORY + "h2-temp");
        new H2Writer(h2Connection, datastore).write();
        h2Connection.close();
    }

}

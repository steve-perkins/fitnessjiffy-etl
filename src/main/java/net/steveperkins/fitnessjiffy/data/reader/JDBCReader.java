package net.steveperkins.fitnessjiffy.data.reader;

import com.google.common.base.Preconditions;
import net.steveperkins.fitnessjiffy.data.model.Datastore;

import java.sql.Connection;

public abstract class JDBCReader {

    protected static final String EXERCISES_JSON_PATH = "/exercises.json";

    protected Connection connection;

    public JDBCReader(Connection connection) {
        Preconditions.checkNotNull(connection);

        this.connection = connection;
    }

    public abstract Datastore read() throws Exception;  // TODO: Create custom Exception wrapper type?

}

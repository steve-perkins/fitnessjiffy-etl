package net.steveperkins.fitnessjiffy.data.writer;

import com.google.common.base.Preconditions;
import net.steveperkins.fitnessjiffy.data.model.Datastore;

import java.sql.Connection;

public abstract class JDBCWriter {

    protected Connection connection;
    protected Datastore datastore;

    public JDBCWriter(Connection connection, Datastore datastore) {
        Preconditions.checkNotNull(connection);
        Preconditions.checkNotNull(datastore);

        this.connection = connection;
        this.datastore = datastore;
    }

    public abstract void write() throws Exception;

}

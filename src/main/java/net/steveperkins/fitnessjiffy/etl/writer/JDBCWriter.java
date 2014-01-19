package net.steveperkins.fitnessjiffy.etl.writer;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;

import java.sql.Connection;

public abstract class JDBCWriter {

    protected Connection connection;
    protected Datastore datastore;

    public JDBCWriter(Connection connection, Datastore datastore) {
        if(connection == null) throw new NullPointerException();
        if(datastore == null) throw new NullPointerException();
        this.connection = connection;
        this.datastore = datastore;
    }

    public abstract void write() throws Exception;

}

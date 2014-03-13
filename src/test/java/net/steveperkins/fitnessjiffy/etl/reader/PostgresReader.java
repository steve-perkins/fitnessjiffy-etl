package net.steveperkins.fitnessjiffy.etl.reader;

import java.sql.Connection;

public class PostgresReader extends JDBCReader {

    public PostgresReader(Connection connection) {
        super(connection);
    }

}

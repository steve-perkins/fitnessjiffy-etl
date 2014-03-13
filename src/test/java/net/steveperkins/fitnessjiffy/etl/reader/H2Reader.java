package net.steveperkins.fitnessjiffy.etl.reader;

import java.sql.Connection;

public class H2Reader extends JDBCReader {

    public H2Reader(Connection connection) {
        super(connection);
    }

}

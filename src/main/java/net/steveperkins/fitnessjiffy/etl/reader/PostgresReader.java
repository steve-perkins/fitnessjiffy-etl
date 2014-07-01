package net.steveperkins.fitnessjiffy.etl.reader;

import javax.annotation.Nonnull;
import java.sql.Connection;

public class PostgresReader extends JDBCReader {

    public PostgresReader(@Nonnull Connection connection) {
        super(connection);
    }

}

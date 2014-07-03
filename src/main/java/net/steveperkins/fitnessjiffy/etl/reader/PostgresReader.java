package net.steveperkins.fitnessjiffy.etl.reader;

import javax.annotation.Nonnull;
import java.sql.Connection;

public final class PostgresReader extends JDBCReader {

    public PostgresReader(@Nonnull final Connection connection) {
        super(connection);
    }

}

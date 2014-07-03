package net.steveperkins.fitnessjiffy.etl.reader;

import javax.annotation.Nonnull;
import java.sql.Connection;

public final class H2Reader extends JDBCReader {

    public H2Reader(@Nonnull final Connection connection) {
        super(connection);
    }

}

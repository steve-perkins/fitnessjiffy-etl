package net.steveperkins.fitnessjiffy.etl.reader;

import javax.annotation.Nonnull;
import java.sql.Connection;

public class H2Reader extends JDBCReader {

    public H2Reader(@Nonnull Connection connection) {
        super(connection);
    }

}

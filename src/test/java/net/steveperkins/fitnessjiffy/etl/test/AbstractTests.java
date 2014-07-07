package net.steveperkins.fitnessjiffy.etl.test;

import javax.annotation.Nonnull;
import java.io.File;

public abstract class AbstractTests {

    protected static final int EXPECTED_JSON_STRING_LENGTH = 3477849;
    protected static final int EXPECTED_JSON_FILE_LENGTH = 3477996;
    protected final String CURRENT_WORKING_DIRECTORY = this.getClass().getProtectionDomain().getCodeSource().getLocation().getFile();

    protected void cleanFileInWorkingDirectory(@Nonnull final String name) throws Exception {
        final File theFile = new File(CURRENT_WORKING_DIRECTORY + name);
        if (theFile.exists() && !theFile.delete()) {
            throw new Exception("There is an existing file " + theFile.getCanonicalPath()
                    + " which can't be deleted for some reason.  Please delete this file manually.");
        }
    }

}

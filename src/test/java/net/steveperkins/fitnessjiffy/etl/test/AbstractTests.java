package net.steveperkins.fitnessjiffy.etl.test;

import java.io.File;

public class AbstractTests {

    protected static final int EXPECTED_JSON_STRING_LENGTH = 3452626;
    protected static final int EXPECTED_JSON_FILE_LENGTH = 3452773;
    protected final String CURRENT_WORKING_DIRECTORY = this.getClass().getProtectionDomain().getCodeSource().getLocation().getFile();

    protected void cleanFileInWorkingDirectory(String name) throws Exception {
        File theFile = new File(CURRENT_WORKING_DIRECTORY + name);
        if(theFile.exists()) {
            if(!theFile.delete()) {
                throw new Exception("There is an existing file " + theFile.getCanonicalPath()
                        + " which can't be deleted for some reason.  Please delete this file manually.");
            }
        }
    }

}

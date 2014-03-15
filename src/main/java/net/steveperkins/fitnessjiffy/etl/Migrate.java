package net.steveperkins.fitnessjiffy.etl;

import net.steveperkins.fitnessjiffy.etl.model.Datastore;
import net.steveperkins.fitnessjiffy.etl.reader.H2Reader;
import net.steveperkins.fitnessjiffy.etl.reader.JDBCReader;
import net.steveperkins.fitnessjiffy.etl.reader.LegacySQLiteReader;
import net.steveperkins.fitnessjiffy.etl.reader.PostgresReader;
import net.steveperkins.fitnessjiffy.etl.writer.H2Writer;
import net.steveperkins.fitnessjiffy.etl.writer.JDBCWriter;
import net.steveperkins.fitnessjiffy.etl.writer.LegacySQLiteWriter;
import net.steveperkins.fitnessjiffy.etl.writer.PostgresWriter;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.sql.Connection;
import java.sql.DriverManager;

public class Migrate {

    public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        Class.forName("org.h2.Driver");
        Class.forName("org.postgresql.Driver");

        Options options = createOptions();
        try {
            CommandLineParser parser = new BasicParser();
            CommandLine commandLine = parser.parse(options, args);

            if(commandLine.hasOption("h")) {
                showHelp(options);
            }
            String inputType        = commandLine.getOptionValue("i") != null ? commandLine.getOptionValue("i").trim().toLowerCase() : "";
            String inputLocation    = commandLine.getOptionValue("l") != null ? commandLine.getOptionValue("l").trim().toLowerCase() : "";
            String inputUsername    = commandLine.getOptionValue("u") != null ? commandLine.getOptionValue("u").trim().toLowerCase() : "";
            String inputPassword    = commandLine.getOptionValue("p") != null ? commandLine.getOptionValue("p").trim().toLowerCase() : "";
            String outputType       = commandLine.getOptionValue("O") != null ? commandLine.getOptionValue("O").trim().toLowerCase() : "";
            String outputLocation   = commandLine.getOptionValue("L") != null ? commandLine.getOptionValue("L").trim().toLowerCase() : "";
            String outputUsername   = commandLine.getOptionValue("U") != null ? commandLine.getOptionValue("U").trim().toLowerCase() : "";
            String outputPassword   = commandLine.getOptionValue("P") != null ? commandLine.getOptionValue("P").trim().toLowerCase() : "";
            if(inputType.isEmpty() || outputType.isEmpty() || inputLocation.isEmpty() || inputLocation.isEmpty() || outputLocation.isEmpty()) {
                System.out.println("\nYou must specify a type and location for the import database and destination database\n");
                showHelp(options);
            }
            if((!inputType.equals("sqlite") && !inputType.equals("h2") && !inputType.equals("postgres"))
                    || (!outputType.equals("sqlite") && !outputType.equals("h2") && !outputType.equals("postgres"))
                    ) {
                System.out.println("\nImport and destination database must be of type \"sqlite\", \"h2\", or \"postgres\"\n");
                showHelp(options);
            }

            Datastore datastore = importDatastore(inputType, inputLocation, inputUsername, inputPassword);
            writeDatastore(outputType, outputLocation, outputUsername, outputPassword, datastore);
            System.out.println("Done!");
        } catch (Exception e) {
            e.printStackTrace();
            showHelp(options);
        }
    }

    private static Options createOptions() {
        Options options = new Options();
        options.addOption("i", "input-type", true, "import type [sqlite, h2, postgres]");
        options.addOption("l", "input-location", true, "location of import database... filesystem path for SQLite or H2, \"host[:port]/database_name\" for PostgreSQL");
        options.addOption("u", "input-username", true, "import username");
        options.addOption("p", "input-password", true, "import password");
        options.addOption("O", "output-type", true, "destination type [sqlite, h2, postgres]");
        options.addOption("L", "output-location", true, "location of destination database... filesystem path for SQLite or H2, \"host[:port]/database_name\" for PostgreSQL");
        options.addOption("U", "output-username", true, "destination username");
        options.addOption("P", "output-password", true, "destination password");
        options.addOption("h", "help", false, "Help");
        return options;
    }

    private static void showHelp(Options options) {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("help", options);
        System.exit(-1);
    }

    private static Datastore importDatastore(String type, String location, String username, String password) {
        try {
            JDBCReader reader = null;
            switch(type) {
                case "sqlite" :
                    Connection connection = DriverManager.getConnection("jdbc:sqlite:" + location);
                    reader = new LegacySQLiteReader(connection);
                    break;
                case "h2" :
                    connection = DriverManager.getConnection("jdbc:h2:" + location);
                    reader = new H2Reader(connection);
                    break;
                case "postgres" :
                    connection = DriverManager.getConnection("jdbc:postgresql://" + location, username, password);
                    reader = new PostgresReader(connection);
                    break;
                default : break;
            }
            return reader.read();
        } catch (Exception e) {
            System.out.println("Cannot to load from import database: [" + location + "]");
            e.printStackTrace();
            return null;
        }
    }

    private static void writeDatastore(String type, String location, String username, String password, Datastore datastore) {
        try {
            JDBCWriter writer = null;
            switch(type) {
                case "sqlite" :
                    Connection connection = DriverManager.getConnection("jdbc:sqlite:" + location);
                    writer = new LegacySQLiteWriter(connection, datastore);
                    break;
                case "h2" :
                    connection = DriverManager.getConnection("jdbc:h2:" + location);
                    writer = new H2Writer(connection, datastore);
                    break;
                case "postgres" :
                    connection = DriverManager.getConnection("jdbc:postgresql://" + location, username, password);
                    writer = new PostgresWriter(connection, datastore);
                    break;
                default :  break;
            }
            writer.write();
        } catch (Exception e) {
            System.out.println("Cannot to write to destination database: [" + location + "]");
            e.printStackTrace();
        }
    }
}

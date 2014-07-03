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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

public final class Migrate {

    public static void main(final String[] args) throws ClassNotFoundException {
        Class.forName("org.sqlite.JDBC");
        Class.forName("org.h2.Driver");
        Class.forName("org.postgresql.Driver");

        final Options options = createOptions();
        try {
            final CommandLineParser parser = new BasicParser();
            final CommandLine commandLine = parser.parse(options, args);

            if (commandLine.hasOption("h")) {
                showHelp(options);
            }
            final String inputType = commandLine.getOptionValue("i") != null ? commandLine.getOptionValue("i").trim().toLowerCase() : "";
            final String inputLocation = commandLine.getOptionValue("l") != null ? commandLine.getOptionValue("l").trim().toLowerCase() : "";
            final String inputUsername = commandLine.getOptionValue("u") != null ? commandLine.getOptionValue("u").trim().toLowerCase() : "";
            final String inputPassword = commandLine.getOptionValue("p") != null ? commandLine.getOptionValue("p").trim().toLowerCase() : "";
            final String outputType = commandLine.getOptionValue("O") != null ? commandLine.getOptionValue("O").trim().toLowerCase() : "";
            final String outputLocation = commandLine.getOptionValue("L") != null ? commandLine.getOptionValue("L").trim().toLowerCase() : "";
            final String outputUsername = commandLine.getOptionValue("U") != null ? commandLine.getOptionValue("U").trim().toLowerCase() : "";
            final String outputPassword = commandLine.getOptionValue("P") != null ? commandLine.getOptionValue("P").trim().toLowerCase() : "";
            if (inputType.isEmpty() || outputType.isEmpty() || inputLocation.isEmpty() || inputLocation.isEmpty() || outputLocation.isEmpty()) {
                System.out.println("\nYou must specify a type and location for the import database or JSON file, and the destination database or JSON file\n");
                showHelp(options);
            }
            if ((!inputType.equals("json") && !inputType.equals("sqlite") && !inputType.equals("h2") && !inputType.equals("postgres"))
                    || (!outputType.equals("json") && !outputType.equals("sqlite") && !outputType.equals("h2") && !outputType.equals("postgres"))
                    ) {
                System.out.println("\nImport and destination database must be of type \"json\", \"sqlite\", \"h2\", or \"postgres\"\n");
                showHelp(options);
            }

            final Datastore datastore = importDatastore(inputType, inputLocation, inputUsername, inputPassword);
            writeDatastore(outputType, outputLocation, outputUsername, outputPassword, datastore);
            System.out.println("Migrated \"" + inputType + "\" data at \"" + inputLocation + "\" to \"" + outputType + "\" at \"" + outputLocation + "\"");
        } catch (Exception e) {
            e.printStackTrace();
            showHelp(options);
        }
    }

    @Nonnull
    private static Options createOptions() {
        final Options options = new Options();
        options.addOption("i", "input-type", true, "import type [json, sqlite, h2, postgres]");
        options.addOption("l", "input-location", true, "location of JSON backup file, or import database... filesystem path for SQLite or H2, \"host[:port]/database_name\" for PostgreSQL");
        options.addOption("u", "input-username", true, "import username (if applicable)");
        options.addOption("p", "input-password", true, "import password (if applicable)");
        options.addOption("O", "output-type", true, "destination type [json, sqlite, h2, postgres]");
        options.addOption("L", "output-location", true, "location of JSON backup file, or destination database... filesystem path for SQLite or H2, \"host[:port]/database_name\" for PostgreSQL");
        options.addOption("U", "output-username", true, "destination username (if applicable)");
        options.addOption("P", "output-password", true, "destination password (if applicable)");
        options.addOption("h", "help", false, "Help");
        return options;
    }

    private static void showHelp(@Nonnull final Options options) {
        final HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp("help", options);
        System.exit(-1);
    }

    @Nullable
    private static Datastore importDatastore(
            @Nonnull final String type,
            @Nonnull final String location,
            @Nonnull final String username,
            @Nonnull final String password
    ) {
        try {
            Datastore datastore = null;
            switch (type) {
                case "json":
                    final File jsonFile = new File(location);
                    datastore = Datastore.fromJSONFile(jsonFile);
                    break;
                case "sqlite":
                    Connection connection = DriverManager.getConnection("jdbc:sqlite:" + location);
                    JDBCReader reader = new LegacySQLiteReader(connection);
                    datastore = reader.read();
                    break;
                case "h2":
                    connection = DriverManager.getConnection("jdbc:h2:" + location);
                    reader = new H2Reader(connection);
                    datastore = reader.read();
                    break;
                case "postgres":
                    connection = DriverManager.getConnection("jdbc:postgresql://" + location, username, password);
                    reader = new PostgresReader(connection);
                    datastore = reader.read();
                    break;
                default:
                    break;
            }
            return datastore;
        } catch (Exception e) {
            System.out.println("Cannot to load from import: [" + location + "]");
            e.printStackTrace();
            return null;
        }
    }

    private static void writeDatastore(
            @Nonnull final String type,
            @Nonnull final String location,
            @Nonnull final String username,
            @Nonnull final String password,
            @Nonnull final Datastore datastore
    ) {
        try {
            switch (type) {
                case "json":
                    final File jsonFile = new File(location);
                    datastore.toJSONFile(jsonFile);
                    break;
                case "sqlite":
                    Connection connection = DriverManager.getConnection("jdbc:sqlite:" + location);
                    JDBCWriter writer = new LegacySQLiteWriter(connection, datastore);
                    writer.write();
                    break;
                case "h2":
                    connection = DriverManager.getConnection("jdbc:h2:" + location);
                    writer = new H2Writer(connection, datastore);
                    writer.write();
                    break;
                case "postgres":
                    connection = DriverManager.getConnection("jdbc:postgresql://" + location, username, password);
                    writer = new PostgresWriter(connection, datastore);
                    writer.write();
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            System.out.println("Cannot to write to destination: [" + location + "]");
            e.printStackTrace();
        }
    }
}

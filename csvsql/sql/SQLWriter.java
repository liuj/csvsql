package csvsql.sql;

import csvsql.csv.CSVReader;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Writes CSV file to SQLite database.
 * <p>
 * The CSV file must have a header.
 * The CSV file may contain records that differ in the number of fields.
 * A record is considered valid if the record has
 * the same number of fields as the header.
 * A record whose number of fields differs from the header
 * is considered invalid, and will be ignored.
 * Invalid records are logged to a file.
 * <p>
 * Recommended usage:
 * <pre>
 * {@code
 * try (SQLWriter writer = new SQLWriter(database)) {
 *     writer.insertRecords(csv, table);
 * } catch (IOException | SQLException x) {
 *     System.err.format("Exception: %s%n", x);
 * }
 * }
 * </pre>
 *
 * @version 0.01, 10/04/19
 */
public class SQLWriter implements Closeable {
    
    /** Path to SQLite database. **/
    private String database;
    
    /** Connection to SQLite database. **/
    private Connection connection;
    
    /** Size of a batch of SQL inserts. */
    private int batchSize;
    
    /**
     * Construct an <code>SQLWriter</code>.
     *
     * @param database path to SQLite database
     * @throws SQLException if error in connecting to database
     */
    public SQLWriter(String database) throws SQLException {
        this(database, 1000);
    }
    
    /**
     * Construct an <code>SQLWriter</code>.
     *
     * @param database path to SQLite database
     * @param batchSize size of a batch of SQL inserts
     * @throws IllegalArgumentException if <code>batchSize &le; 0</code>
     * @throws SQLException if error in connecting to database
     */
    public SQLWriter(String database, int batchSize) throws SQLException {
        if (batchSize <= 0)
            throw new IllegalArgumentException("Batch size must be positive.");
        this.database = database;
        this.batchSize = batchSize;
        connection = DriverManager.getConnection("jdbc:sqlite:" + database);
        connection.setAutoCommit(false);
    }
    
    /**
     * Close the database connection.
     */
    @Override
    public void close() {
        try {
            if (connection != null)
                connection.close();
        } catch (SQLException e) {
            System.err.format("SQLException: %s%n", e);
        }
    }
    
    /**
     * Insert records from CSV file into database table.
     * The number of fields/columns is determined by the CSV file header.
     * A record whose number of fields differs from the header
     * is considered invalid, and will be ignored.
     * Invalid records are logged to a file.
     *
     * @param csv CSV filename
     * @param table database table name
     * @return summary message
     * @throws IOException if an I/O error occurs
     * @throws SQLException if a database error occurs
     */
    public String insertRecords(String csv, String table) throws IOException, SQLException {
        System.out.println("Reading from: " + csv);
        System.out.println("Writing to: table " + table
                                                + " in " + database);
        
        CSVReader reader = null;
        String invalidRecordsFileName;
        if (csv.endsWith(".csv"))
            invalidRecordsFileName = csv.substring(0, csv.lastIndexOf('.')) + "-invalid.csv";
        else
            invalidRecordsFileName = csv + "-invalid.csv";
        BufferedWriter writer = null;
        
        PreparedStatement pStatement = null;
        
        int validRecords = 0;
        int invalidRecords = 0;
        
        try {
            reader = new CSVReader(csv);
            
            String header = reader.next();
            ArrayList<String> columnNames = CSVReader.parseRecord(header);
            int columns = columnNames.size();
            
            String placeholders = "?,".repeat(columns);
            placeholders = placeholders.substring(0, placeholders.length() - 1);
            String statement = "INSERT INTO " + table
                                              + " VALUES("
                                              + placeholders + ")";
            pStatement = connection.prepareStatement(statement);
            
            int inserts = 0;
            
            while (reader.hasNext()) {
                String record = reader.next();
                if (record.equals(""))
                    continue;
                ArrayList<String> values = CSVReader.parseRecord(record);
                if (values.size() == columns) {
                    for (int i = 1; i <= columns; ++i)
                        pStatement.setString(i, values.get(i - 1));
                    pStatement.addBatch();
                    inserts += 1;
                    if (inserts == batchSize) {
                        pStatement.executeBatch();
                        connection.commit();
                        inserts = 0;
                    }
                    validRecords += 1;
                } else {
                    if (writer == null)
                        writer = Files.newBufferedWriter(Paths.get(invalidRecordsFileName));
                    writer.write(record, 0, record.length());
                    writer.newLine();
                    invalidRecords += 1;
                }
            }
            
            if (inserts > 0) {
                pStatement.executeBatch();
                connection.commit();
            }
        } finally {
            if (reader != null)
                try {
                    reader.close();
                } catch (IOException e) {
                    System.err.format("IOException: %s%n", e);
                }
            if (writer != null)
                try {
                    writer.close();
                } catch (IOException e) {
                    System.err.format("IOException: %s%n", e);
                }
            if (pStatement != null)
                try {
                    pStatement.close();
                } catch (SQLException e) {
                    System.err.format("SQLException: %s%n", e);
                }
        }
        
        String message = "Total records in CSV file: " + (validRecords + invalidRecords)
                             + "\nValid records in CSV file: " + validRecords
                             + "\nInvalid records in CSV file: " + invalidRecords;
        if (invalidRecords > 0)
            message += "\nInvalid CSV records saved to: " + invalidRecordsFileName;
        return message;
    }
    
    /**
     * Read from CSV file and write to SQLite database.
     *
     * @param args csv, database, table
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.out.println("Usage: java SQLWriter <csv> <database> <table>");
            System.exit(1);
        }
        try (SQLWriter writer = new SQLWriter(args[1])) {
            System.out.println(writer.insertRecords(args[0], args[2]));
        } catch (IOException | SQLException x) {
            System.err.format("Exception: %s%n", x);
        }
    }
}

package csvsql.csv;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An iterator over the records in a CSV file.
 * <p>
 * The CSV file must satisfy the following properties:
 * <ul>
 * <li>Every field containing embedded commas is
 *     enclosed in double-quotes.
 * <li>Every field containing embedded double-quote characters is
 *     enclosed in double-quotes.
 * <li>Every embedded double-quote character is represented as
 *     a pair of double-quote characters.
 * <li>Every field containing embedded line breaks is
 *     enclosed in double-quotes.
 * </ul>
 * <p>
 * The CSV file may contain records that differ in the number of fields.
 * <p>
 * Recommended usage:
 * <pre>
 * {@code
 * try (CSVReader reader = new CSVReader(csv)) {
 *     while (reader.hasNext()) {
 *         String record = reader.next();
 *         ArrayList<String> values = CSVReader.parseRecord(record);
 *         // ...
 *     }
 * } catch (IOException x) {
 *     System.err.format("IOException: %s%n", x);
 * }
 * }
 * </pre>
 *
 * @version 0.01, 10/04/19
 */
public class CSVReader implements Iterator<String>, Closeable {
    
    /**
     * CSV file reader.
     */
    private BufferedReader reader;
    
    /**
     * Next record in CSV file.
     */
    private String nextRecord;
    
    /**
     * Reader state.
     */
    private boolean open = false;
    
    /**
     * Initial capacity of a temporary string builder
     * holding a record that is being read.
     */
    private int capacity = 8192;
    
    /**
     * Construct a <code>CSVReader</code>.
     *
     * @param csv CSV filename
     * @throws IOException if an I/O error occurs
     */
    public CSVReader(String csv) throws IOException {
        reader = Files.newBufferedReader(Paths.get(csv));
        open = true;
    }
    
    /**
     * Return true if there are more records to be read.
     *
     * @return {@code true} if there are more records to be read
     */
    @Override
    public boolean hasNext() {
        if (!open)
            return false;
        if (nextRecord != null)
            return true;
        try {
            nextRecord = readRecord();
        } catch (IOException e) {
            System.err.format("IOException: %s%n", e);
        }
        return nextRecord != null ? true : false;
    }
    
    /**
     * Return next record.
     *
     * @return next record
     * @throws NoSuchElementException if no more records
     */
    @Override
    public String next() {
        if (!hasNext())
            throw new NoSuchElementException("No more records.");
        String record = nextRecord;
        nextRecord = null;
        return record;
    }
    
    /**
     * Close the reader.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        if (reader != null)
            reader.close();
        nextRecord = null;
        open = false;
    }
    
    /**
     * Read next record in CSV file.
     *
     * @return next record in CSV file
     * @throws IOException if an I/O error occurs
     */
    private String readRecord() throws IOException {
        int c = reader.read();
        if (c == -1)
            return null;
        StringBuilder record = new StringBuilder(capacity);
        boolean foundEnd = false;
        while (!foundEnd)
            switch (c) {
                case '\n':
                    foundEnd = true;
                    break;
                case '\r':
                    c = reader.read();
                    break;
                case '"':
                    record.append((char)c);
                    c = reader.read();
                    boolean quoteEnd = false;
                    while (true) {
                        switch (c) {
                            case '"':
                                record.append((char)c);
                                c = reader.read();
                                if (c == '"') {
                                    record.append((char)c);
                                    c = reader.read();
                                } else
                                    quoteEnd = true;
                                break;
                            default:
                                record.append((char)c);
                                c = reader.read();
                                break;
                        }
                        if (quoteEnd)
                            break;
                    }
                    break;
                default:
                    record.append((char)c);
                    c = reader.read();
                    break;
            }
        capacity = Math.max(capacity, record.capacity());
        return record.toString();
    }
    
    /**
     * Parse a single record from a CSV file.
     *
     * @param record the record
     * @return parsed record values
     */
    public static ArrayList<String> parseRecord(String record) {
        ArrayList<String> values = new ArrayList<>();
        if (record.equals(""))
            return values;
        int s = 0;
        int e = 0;
        while (true)
            switch (record.charAt(s)) {
                case ',':
                    values.add(null);
                    if (s + 1 == record.length()) {
                        values.add(null);
                        return values;
                    }
                    s += 1;
                    break;
                case '"':
                    s += 1;
                    e = s;
                    boolean pair = false;
                    while (true)
                        if (record.charAt(e) == '"') {
                            if (e + 1 < record.length()) {
                                if (record.charAt(e + 1) == '"') {
                                    pair = true;
                                    e += 2;
                                    continue;
                                }
                                if (pair)
                                    values.add(record.substring(s, e)
                                               .replace("\"\"", "\""));
                                else
                                    values.add(record.substring(s, e));
                                if (e + 2 == record.length()) {
                                    values.add(null);
                                    return values;
                                } else {
                                    s = e + 2;
                                    break;
                                }
                            } else {
                                if (pair)
                                    values.add(record.substring(s, e)
                                               .replace("\"\"", "\""));
                                else
                                    values.add(record.substring(s, e));
                                return values;
                            }
                        } else
                            e += 1;
                    break;
                default:
                    e = s + 1;
                    while (e < record.length() && record.charAt(e) != ',')
                        e += 1;
                    values.add(record.substring(s, e));
                    if (e == record.length())
                        return values;
                    if (e + 1 == record.length()) {
                        values.add(null);
                        return values;
                    }
                    s = e + 1;
                    break;
            }
    }
}

# csvsql

Utilities for reading from CSV and writing to SQLite.

Supports CSV files containing records with variable number of fields.

2 programs are included:
* `CSVReader.java`, an iterator over the records in a CSV file.
* `SQLWriter.java`, a command-line utility for writing CSV files to SQLite databases.

**Installation**

`SQLWriter` requires a [JDBC driver](https://github.com/xerial/sqlite-jdbc).

On Linux, compile everything by typing at the command-line:
```shell
javac csvsql/sql/SQLWriter.java
```

**Usage**

The recommended usage for `CSVReader` is:
```java
try (CSVReader reader = new CSVReader("mydata.csv")) {
    while (reader.hasNext()) {
        String record = reader.next();
        ArrayList<String> values = CSVReader.parseRecord(record);
        // ...
    }
} catch (IOException x) {
    System.err.format("IOException: %s%n", x);
}
```
The recommended usage for `SQLWriter` is:
```java
try (SQLWriter writer = new SQLWriter("mydatabase.db")) {
    writer.insertRecords("mydata.csv", "mytable");
} catch (IOException | SQLException x) {
    System.err.format("Exception: %s%n", x);
}
```
To use `SQLWriter` from the Linux command-line, type:
```shell
java -cp '.:sqlite-jdbc-VERSION.jar' csvsql/sql/SQLWriter <csv> <database> <table>
```

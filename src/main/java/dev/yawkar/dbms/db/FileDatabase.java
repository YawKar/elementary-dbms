package dev.yawkar.dbms.db;

import dev.yawkar.dbms.exception.DatabaseFileMetaMissingException;
import dev.yawkar.dbms.exception.NoSuchTableNameException;
import dev.yawkar.dbms.specification.TableSpecification;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

public class FileDatabase implements Database {

    private final File dbFile;
    private final String uri;
    private final List<Table> tables = new ArrayList<>();

    public FileDatabase(String uri, File dbFile, boolean newDbFile) {
        this.dbFile = dbFile;
        this.uri = uri;
        if (newDbFile) {
            initDb();
        }
        extractMetadata();
    }

    private void initDb() {
        try (FileWriter fileWriter = new FileWriter(dbFile)) {
            fileWriter.write("!startmeta\n");
            fileWriter.write("version:1\n");
            fileWriter.write("tables:1\n");
            fileWriter.write("!endmeta\n");
            fileWriter.write("!starttables\n");
            fileWriter.write("students\n");
            fileWriter.write("1\n");
            fileWriter.write("2\n");
            fileWriter.write("student_id\n");
            fileWriter.write("long\n");
            fileWriter.write("name\n");
            fileWriter.write("string\n");
            fileWriter.write("!endtables\n");
            fileWriter.write("!startdata\n");
            fileWriter.write("1 1337 yawkar\n");
            fileWriter.write("!enddata\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void extractMetadata() {
        Map<String, String> metadata = new HashMap<>();
        try (var lines = Files.lines(dbFile.toPath())) {
            var iterator = lines.iterator();
            while (iterator.hasNext()) {
                String line = iterator.next();
                if (line.equals("!startmeta")) {
                    processMetaBlock(iterator, metadata);
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (!metadata.containsKey("version"))
            throw new DatabaseFileMetaMissingException("version");
        if (!metadata.containsKey("tables"))
            throw new DatabaseFileMetaMissingException("tables");
        int tablesNumber = Integer.parseInt(metadata.get("tables"));
        extractTables(tablesNumber);
    }

    private void extractTables(int tablesNumber) {
        try (var lines = Files.lines(dbFile.toPath())) {
            var iterator = lines.iterator();
            while (iterator.hasNext()) {
                String line = iterator.next();
                if (line.equals("!starttables")) {
                    processTablesBlock(iterator);
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processTablesBlock(Iterator<String> iterator) {
        while (iterator.hasNext()) {
            String tableName = iterator.next();
            if (tableName.equals("!endtables")) {
                break;
            }
            processTable(tableName, iterator);
        }
    }

    private void processTable(String tableName, Iterator<String> iterator) {
        int tableId = Integer.parseInt(iterator.next());
        int columnsNumber = Integer.parseInt(iterator.next());
        SimpleTable table = new SimpleTable(tableName, dbFile, tableId);
        for (int i = 0; i < columnsNumber; ++i) {
            SimpleColumn column = new SimpleColumn();
            column.label = iterator.next();
            column.index = i;
            column.type = iterator.next();
            table.columns.add(column);
        }
        tables.add(table);
    }


    private void processMetaBlock(Iterator<String> iterator, Map<String, String> metadata) {
        while (iterator.hasNext()) {
            String line = iterator.next();
            if (line.equals("!endmeta")) {
                break;
            }
            String[] entry = line.split(":");
            metadata.put(entry[0], entry[1]);
        }
    }

    @Override
    public String getUri() {
        return uri;
    }

    @Override
    public Table createTable(TableSpecification tableSpecification) {
        System.out.println("Not implemented yet");
        return null;
    }

    @Override
    public Table getTable(String tableName) {
        return tables.stream()
                .filter(t -> t.getName().equals(tableName))
                .findAny()
                .orElseThrow(() -> new NoSuchTableNameException(tableName));
    }

    @Override
    public List<Table> getTables() {
        return tables;
    }

    @Override
    public void dropTable(String tableName) {
        Table table = tables.stream()
                .filter(t -> t.getName().equals(tableName))
                .findAny()
                .orElseThrow(() -> new NoSuchTableNameException(tableName));
        System.out.println("Not implemented yet");
    }

    @Override
    public void dropTable(Table tableName) {
        System.out.println("Not implemented yet");
    }
}

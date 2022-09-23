package dev.yawkar.dbms.db;

import dev.yawkar.dbms.exception.InvalidNumberOfValuesException;
import dev.yawkar.dbms.exception.PrimaryKeyConstraintViolationException;
import dev.yawkar.dbms.specification.CriteriaSpecification;
import dev.yawkar.dbms.specification.UpdateSpecification;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleTable implements Table {

    String name;
    File dbFile;
    int tableId;
    List<Column> columns = new ArrayList<>();

    SimpleTable() {}

    SimpleTable(String name) {
        this.name = name;
    }

    SimpleTable(String name, File dbFile) {
        this(name);
        this.dbFile = dbFile;
    }

    SimpleTable(String name, File dbFile, int tableId) {
        this(name, dbFile);
        this.tableId = tableId;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public List<Row> getRows() {
        try (var lines = Files.lines(dbFile.toPath())) {
            var iterator = lines.iterator();
            while (iterator.hasNext()) {
                String line = iterator.next();
                if (line.equals("!startdata")) {
                    return filterAllRowsByTableId(iterator);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private List<Row> filterAllRowsByTableId(Iterator<String> iterator) {
        List<Row> result = new ArrayList<>();
        while (iterator.hasNext()) {
            String line = iterator.next();
            if (line.equals("!enddata")) {
                break;
            }
            Scanner scanner = new Scanner(line);
            int rowsTableId = scanner.nextInt();
            if (rowsTableId == tableId) {
                SimpleRow simpleRow = new SimpleRow();
                while (scanner.hasNext()) {
                    simpleRow.elements.add(new SimpleRowElement(scanner.next()));
                }
                result.add(simpleRow);
            }
        }
        return result;
    }

    @Override
    public List<Row> getQueriedRows(CriteriaSpecification criteria) {
        criteria.setup(this);
        return getRows().stream().filter(criteria::examine).collect(Collectors.toList());
    }

    @Override
    public void insertRow(Object...values) {
        if (values.length != columns.size()) {
            throw new InvalidNumberOfValuesException("Expected %d values but got %d".formatted(columns.size(), values.length));
        }
        // check if insertion violates PK uniqueness
        Column pkColumn = columns.stream().filter(Column::isPk).findAny().get();
        if (getRows().stream().anyMatch(r -> r.get(pkColumn.getIndex()).asString().equals(values[0].toString()))) {
            throw new PrimaryKeyConstraintViolationException("Row with PK %s(%s) already exists in table '%s'"
                    .formatted(pkColumn.getLabel(), values[0], name));
        }
        try {
            File temporaryDbFile = Files.createFile(Path.of(dbFile.getPath() + ".temp")).toFile();
            try (var lines = Files.lines(dbFile.toPath());
                 FileWriter fileWriter = new FileWriter(temporaryDbFile)) {
                var iterator = lines.iterator();
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (line.equals("!enddata")) {
                        fileWriter.write(Integer.toString(tableId));
                        fileWriter.write(' ');
                        fileWriter.write(Arrays.stream(values).map(Object::toString).collect(Collectors.joining(" ")));
                        fileWriter.write('\n');
                    }
                    fileWriter.write(line);
                    fileWriter.write('\n');
                }
            }
            if (!dbFile.delete()) {
                throw new RuntimeException("Cannot delete old db file '%s'".formatted(dbFile.getName()));
            }
            if (!temporaryDbFile.renameTo(dbFile)) {
                throw new RuntimeException("Cannot rename '%s' to '%s'".formatted(temporaryDbFile.getName(), dbFile.getName()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteQueriedRows(CriteriaSpecification criteria) {
        criteria.setup(this);
        Set<String> pkKeysToDelete = new HashSet<>();
        Column pkColumn = columns.stream().filter(Column::isPk).findAny().get();
        for (var row : getQueriedRows(criteria)) {
            pkKeysToDelete.add(row.get(pkColumn.getIndex()).asString());
        }
        try {
            File temporaryDbFile = Files.createFile(Path.of(dbFile.getPath() + ".temp")).toFile();
            try (var lines = Files.lines(dbFile.toPath());
                 FileWriter tempWriter = new FileWriter(temporaryDbFile)) {
                var iterator = lines.iterator();
                boolean inDataSection = false;
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (inDataSection) {
                        if (!line.equals("!enddata")) {
                            Scanner rowScanner = new Scanner(line);
                            int rowTableId = rowScanner.nextInt();
                            for (int elemIndex = 0; elemIndex < pkColumn.getIndex(); ++elemIndex)
                                rowScanner.next();
                            String rowPKey = rowScanner.next();
                            if (rowTableId != tableId || !pkKeysToDelete.contains(rowPKey)) {
                                tempWriter.write(line);
                                tempWriter.write('\n');
                            }
                        } else {
                            inDataSection = false;
                            tempWriter.write(line);
                            tempWriter.write('\n');
                        }
                    } else {
                        tempWriter.write(line);
                        tempWriter.write('\n');
                        if (line.equals("!startdata")) {
                            inDataSection = true;
                        }
                    }
                }
            }
            if (!dbFile.delete()) {
                throw new RuntimeException("Cannot delete old db file '%s'".formatted(dbFile.getName()));
            }
            if (!temporaryDbFile.renameTo(dbFile)) {
                throw new RuntimeException("Cannot rename '%s' to '%s'".formatted(temporaryDbFile.getName(), dbFile.getName()));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void updateQueriedRows(CriteriaSpecification criteria, UpdateSpecification update) {
        update.setup(this);
        Set<String> allPKeys = new HashSet<>();
        Column pKeyColumn = columns.stream().filter(Column::isPk).findAny().get();
        // get all keys in the table
        getRows().forEach(r -> allPKeys.add(r.get(pKeyColumn.getIndex()).asString()));
        // get all rows that should be updated
        var rowsToUpdate = getQueriedRows(criteria);
        // delete their keys
        rowsToUpdate.forEach(r -> allPKeys.remove(r.get(pKeyColumn.getIndex()).asString()));
        // update each row via UpdateSpecification
        rowsToUpdate.forEach(update::update);
        // try to add all keys back to the set of all keys
        rowsToUpdate.forEach(r -> {
            if (allPKeys.contains(r.get(pKeyColumn.getIndex()).asString())) {
                throw new PrimaryKeyConstraintViolationException(
                        "After update there will be more than 1 row with the same PK '%s(%s)' in table '%s'"
                                .formatted(pKeyColumn.getLabel(), r.get(pKeyColumn.getIndex()).asString(), name));
            }
            allPKeys.add(r.get(pKeyColumn.getIndex()).asString());
        });
        // delete these rows by criteria
        deleteQueriedRows(criteria);
        // add their updated versions back
        rowsToUpdate.forEach(r -> {
            insertRow(IntStream.range(0, columns.size()).mapToObj(r::get).map(RowElement::asString).toArray());
        });
    }
}

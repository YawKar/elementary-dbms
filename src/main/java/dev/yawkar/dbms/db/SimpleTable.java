package dev.yawkar.dbms.db;

import dev.yawkar.dbms.exception.InvalidNumberOfValuesException;
import dev.yawkar.dbms.exception.PrimaryKeyConstraintViolationException;
import dev.yawkar.dbms.specification.CriteriaSpecification;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

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
    public List<Row> getQueriedRows(CriteriaSpecification criteriaSpecification) {
        criteriaSpecification.setup(this);
        return getRows().stream().filter(criteriaSpecification::examine).collect(Collectors.toList());
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
    public void deleteQueriedRows(CriteriaSpecification criteriaSpecification) {
        criteriaSpecification.setup(this);
        Set<String> pkKeysToDelete = new HashSet<>();
        Column pkColumn = columns.stream().filter(Column::isPk).findAny().get();
        for (var row : getQueriedRows(criteriaSpecification)) {
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
}

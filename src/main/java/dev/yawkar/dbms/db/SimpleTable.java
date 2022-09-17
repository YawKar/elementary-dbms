package dev.yawkar.dbms.db;

import dev.yawkar.dbms.exception.InvalidNumberOfValuesException;
import dev.yawkar.dbms.exception.UnknownDatabaseTypeUriException;
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
}

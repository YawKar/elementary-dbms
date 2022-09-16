package dev.yawkar.dbms.db;

import dev.yawkar.dbms.specification.CriteriaSpecification;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
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
    public void createRow(Row row) {

    }
}

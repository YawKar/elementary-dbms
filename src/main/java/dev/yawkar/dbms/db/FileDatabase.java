package dev.yawkar.dbms.db;

import dev.yawkar.dbms.exception.*;
import dev.yawkar.dbms.specification.TableSpecification;
import dev.yawkar.dbms.specification.criteria.ALL;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.IntStream;

public class FileDatabase implements Database {

    private final File dbFile;
    private final String uri;
    private final List<Table> tables = new ArrayList<>();
    private int tableIdCounter = 0;

    private int getFreeTableId() {
        while (tables.stream().anyMatch(t -> t.getTableId() == tableIdCounter)) {
            ++tableIdCounter;
        }
        return tableIdCounter;
    }

    public FileDatabase(String uri, File dbFile, boolean newDbFile) {
        this.dbFile = dbFile;
        this.uri = uri;
        if (newDbFile) {
            initDb();
        }
        extractMetadata();
        validateStateOfTables();
    }

    private void validateStateOfTables() {
        for (var table : tables) {
            List<Row> rows = table.getRows();
            List<Column> columns = table.getColumns();
            long pkColumns = columns.stream().filter(Column::isPk).count();
            if (pkColumns != 1)
                throw new IllegalTableDefinitionException("Table '%s' contains %d columns with PK (should contain only 1)"
                        .formatted(table.getName(), pkColumns));
            Set<String> primaryKeys = new HashSet<>();
            Column pkColumn = columns.stream().filter(Column::isPk).findAny().get();
            for (Row row : rows) {
                String pkId = row.get(pkColumn.getIndex()).asString();
                if (primaryKeys.contains(pkId))
                    throw new PrimaryKeyConstraintViolationException("More than 1 row with the same PK '%s(%s)' in table '%s'"
                            .formatted(pkColumn.getLabel(), pkId, table.getName()));
                primaryKeys.add(pkId);
            }
        }
    }

    private void initDb() {
        try (FileWriter fileWriter = new FileWriter(dbFile)) {
            fileWriter.write("!startmeta\n");
            fileWriter.write("version:1\n");
            fileWriter.write("tables:1\n");
            fileWriter.write("!endmeta\n");
            fileWriter.write("!starttables\n");
            fileWriter.write("students 1\n");
            fileWriter.write("2\n");
            fileWriter.write("student_id long PK\n");
            fileWriter.write("name string\n");
            fileWriter.write("labs 2\n");
            fileWriter.write("2\n");
            fileWriter.write("lab_id long PK\n");
            fileWriter.write("lab_title string\n");
            fileWriter.write("courses 3\n");
            fileWriter.write("3\n");
            fileWriter.write("course_id long PK\n");
            fileWriter.write("course_applicants long\n");
            fileWriter.write("course_title string\n");
            fileWriter.write("!endtables\n");
            fileWriter.write("!startdata\n");
            fileWriter.write("1 1 yawkar\n");
            fileWriter.write("1 2 didhat\n");
            fileWriter.write("1 4 yakiza\n");
            fileWriter.write("1 3 neon.eagle\n");
            fileWriter.write("2 1 mathematical_analysis\n");
            fileWriter.write("2 2 physics\n");
            fileWriter.write("2 3 programming_in_java\n");
            fileWriter.write("3 1 89 Software_Engineering\n");
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
        extractTables();
        if (tablesNumber != tables.size())
            throw new DatabaseFileMetaMissingException("'tables' in metadata (%d) does not match number of table definitions in database (%d)"
                    .formatted(tablesNumber, tables.size()));
    }

    private void extractTables() {
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
            String tableDefinition = iterator.next();
            if (tableDefinition.equals("!endtables")) {
                break;
            }
            processTable(tableDefinition, iterator);
        }
    }

    private void processTable(String tableDefinition, Iterator<String> iterator) {
        int tableId = Integer.parseInt(tableDefinition.split(" ")[1]);
        String tableName = tableDefinition.split(" ")[0];
        int columnsNumber = Integer.parseInt(iterator.next());
        SimpleTable table = new SimpleTable(tableName, dbFile, tableId);
        for (int i = 0; i < columnsNumber; ++i) {
            SimpleColumn column = new SimpleColumn();
            String columnDefinition = iterator.next();
            Scanner columnScanner = new Scanner(columnDefinition);
            column.label = columnScanner.next();
            column.index = i;
            column.type = columnScanner.next();
            while (columnScanner.hasNext()) {
                String attribute = columnScanner.next();
                switch (attribute) {
                    case "PK" -> column.pk = true;
                    default -> throw new UnknownColumnAttributeException(
                            "Unknown attribute '%s' for column '%s' in table '%s'".formatted(
                                    attribute, column.label, table.getName()
                            ));
                }
            }
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
        SimpleTable table = new SimpleTable(tableSpecification.getName(), dbFile, getFreeTableId());
        IntStream.range(0, tableSpecification.getColumns().size())
                .mapToObj(i -> new SimpleColumn(
                        tableSpecification.getColumn(i).getLabel(),
                        i,
                        tableSpecification.getColumn(i).getType(),
                        tableSpecification.getColumn(i).isPK()))
                .forEach(c -> table.columns.add(c));
        if (tables.stream().anyMatch(t -> t.getName().equals(table.name)))
            throw new TableWithSuchNameAlreadyExistsException(table.getName());
        tables.add(table);
        addTableToDbFile(tableSpecification, table.getTableId());
        return table;
    }

    private void addTableToDbFile(TableSpecification table, int tableId) {
        DBIOUtils.updateMeta("tables", Integer.toString(tables.size()), dbFile);
        DBIOUtils.insertNewTableDefinition(table, tableId, dbFile);
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
        table.deleteQueriedRows(new ALL());
        tables.remove(table);
        DBIOUtils.updateMeta("tables", Integer.toString(tables.size()), dbFile);
        DBIOUtils.deleteTableDefinition(tableName, dbFile);
    }
}

package dev.yawkar.dbms.db;

import dev.yawkar.dbms.specification.TableSpecification;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class DBIOUtils {

    public static void updateMeta(String key, String value, File dbFile) {
        try {
            File temporaryDbFile = Files.createFile(Path.of(dbFile.getPath() + ".temp")).toFile();
            try (var lines = Files.lines(dbFile.toPath());
                 FileWriter fileWriter = new FileWriter(temporaryDbFile)) {
                var iterator = lines.iterator();
                boolean inMetaSection = false;
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (line.equals("!startmeta")) {
                        inMetaSection = true;
                    } else if (line.equals("!endmeta")) {
                        inMetaSection = false;
                    }
                    if (inMetaSection && line.startsWith(key)) {
                        fileWriter.write("%s:%s\n".formatted(key, value));
                    } else {
                        fileWriter.write(line);
                        fileWriter.write('\n');
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

    public static void insertNewTableDefinition(TableSpecification table, int tableId, File dbFile) {
        try {
            File temporaryDbFile = Files.createFile(Path.of(dbFile.getPath() + ".temp")).toFile();
            try (var lines = Files.lines(dbFile.toPath());
                 FileWriter fileWriter = new FileWriter(temporaryDbFile)) {
                var iterator = lines.iterator();
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (line.equals("!endtables")) {
                        fileWriter.write("%s %d\n".formatted(table.getName(), tableId));
                        fileWriter.write("%d\n".formatted(table.getColumns().size()));
                        for (var column : table.getColumns()) {
                            fileWriter.write("%s %s".formatted(column.getLabel(), column.getType()));
                            if (column.isPK())
                                fileWriter.write(" PK");
                            fileWriter.write('\n');
                        }
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

    public static void deleteTableDefinition(String tableName, File dbFile) {
        try {
            File temporaryDbFile = Files.createFile(Path.of(dbFile.getPath() + ".temp")).toFile();
            try (var lines = Files.lines(dbFile.toPath());
                 FileWriter fileWriter = new FileWriter(temporaryDbFile)) {
                var iterator = lines.iterator();
                boolean inTablesDefinitions = false;
                while (iterator.hasNext()) {
                    String line = iterator.next();
                    if (line.equals("!endtables")) {
                        inTablesDefinitions = false;
                    }
                    if (inTablesDefinitions) {
                        if (tableName.equals(line.split(" ")[0])) {
                            skipWritingTableDuringDeletion(iterator);
                        } else {
                            fileWriter.write(line);
                            fileWriter.write('\n');
                            writeTableDuringDeletion(iterator, fileWriter);
                        }
                    } else {
                        fileWriter.write(line);
                        fileWriter.write('\n');
                    }
                    if (line.equals("!starttables")) {
                        inTablesDefinitions = true;
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

    private static void writeTableDuringDeletion(Iterator<String> iterator, FileWriter fileWriter) throws IOException {
        int columnsNumber = Integer.parseInt(iterator.next());
        fileWriter.write(Integer.toString(columnsNumber));
        fileWriter.write('\n');
        for (int i = 0; i < columnsNumber; ++i) {
            fileWriter.write(iterator.next());
            fileWriter.write('\n');
        }
    }

    private static void skipWritingTableDuringDeletion(Iterator<String> iterator) {
        int columnsNumber = Integer.parseInt(iterator.next());
        for (int i = 0; i < columnsNumber; ++i) {
            iterator.next();
        }
    }
}

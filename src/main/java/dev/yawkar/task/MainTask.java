package dev.yawkar.task;

import dev.yawkar.dbms.DBManager;
import dev.yawkar.dbms.ElemDBMSManager;
import dev.yawkar.dbms.db.Database;
import dev.yawkar.dbms.db.Row;
import dev.yawkar.dbms.db.Table;
import dev.yawkar.dbms.specification.criteria.PK;
import dev.yawkar.dbms.specification.definition.ColumnDefinition;
import dev.yawkar.dbms.specification.definition.TableDefinition;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class MainTask {

    public static void run() throws IOException {
        DBManager manager = new ElemDBMSManager();
        // Create database
        Database db = manager.createDatabase("file:taskDB.elmdb");
        // Create table 'students'
        Table students = db.createTable(
                new TableDefinition(
                        "students",
                        new ColumnDefinition("id", "long", true),
                        new ColumnDefinition("name", "string", false),
                        new ColumnDefinition("surname", "string", false)
                )
        );
        // Populate the 'students' table
        try (var lines = Files.lines(Path.of("names.txt"))) {
            int studentId = 0;
            for (var studentEntry : lines.toList()) {
                students.insertRow(studentId++, studentEntry.split(" ")[0], studentEntry.split(" ")[1]);
            }
        }
        // Create table 'variants'
        Table variants = db.createTable(
                new TableDefinition(
                        "variants",
                        new ColumnDefinition("id", "long", true),
                        new ColumnDefinition("filename", "string", false)
                )
        );
        // Populate the 'variants' table
        try (var lines = Files.lines(Path.of("variants.txt"))) {
            int variantId = 0;
            for (var variantEntry : lines.toList()) {
                variants.insertRow(variantId++, variantEntry);
            }
        }
        // Create table 'testing_arrangement'
        Table testingArrangement = db.createTable(
                new TableDefinition(
                        "testing_arrangement",
                        new ColumnDefinition("student_id", "long", true),
                        new ColumnDefinition("variant_id", "long", false)
                )
        );
        // Generate testing arrangement and store it in 'testing_arrangement'
        List<Row> studentsEntries = students.getRows();
        List<Row> variantsEntries = variants.getRows();
        for (var student : studentsEntries) {
            testingArrangement.insertRow(
                    student.get(0).asLong(),
                    variantsEntries.get(ThreadLocalRandom.current().nextInt(0, variantsEntries.size())).get(0).asLong()
            );
        }
        // Write results into 'output.txt' according to 'testing_arrangement'
        try (PrintWriter writer = new PrintWriter("output.txt")) {
            writer.println("full_name path_to_file");
            List<Row> arrangement = testingArrangement.getRows();
            for (var assignment : arrangement) {
                long studentId = assignment.get(0).asLong();
                long variantId = assignment.get(1).asLong();
                Row student = students.getQueriedRows(new PK(studentId)).get(0);
                String studentFullName = student.get(1).asString() + " " + student.get(2).asString();
                String variantFile = variants.getQueriedRows(new PK(variantId)).get(0).get(1).asString();
                writer.printf("%s %s%n", studentFullName, variantFile);
            }
        }
        // Dump db
        manager.dumpDatabase(db);
        // Delete the database
        manager.dropDatabase(db);
    }
}

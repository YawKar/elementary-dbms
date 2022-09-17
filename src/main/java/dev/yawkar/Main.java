package dev.yawkar;

import dev.yawkar.dbms.DBManager;
import dev.yawkar.dbms.ElemDBMSManager;
import dev.yawkar.dbms.db.Database;
import dev.yawkar.dbms.db.Table;

public class Main {
    public static void main(String[] args) {
        DBManager manager = new ElemDBMSManager();
        Database database = manager.getDatabase("file:sample.elmdb");
        for (var table : database.getTables()) {
            System.out.printf("Table: %s%n", table.getName());
            for (var column : table.getColumns()) {
                System.out.printf("%s(%s) ", column.getLabel(), column.getType());
            }
            System.out.println();
            System.out.println("Rows:");
            for (var row : table.getRows()) {
                for (int i = 0; i < table.getColumns().size(); ++i) {
                    System.out.printf("%s ", row.get(i).asString());
                }
                System.out.println();
            }
            System.out.println();
        }

        Table studentsTable = database.getTable("students");
        studentsTable.insertRow(5, "DECAHTHUK");
        studentsTable.getRows().forEach(System.out::println);

        //manager.dropDatabase("file:sample.elmdb");
    }
}
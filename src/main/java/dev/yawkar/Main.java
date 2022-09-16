package dev.yawkar;

import dev.yawkar.dbms.DBManager;
import dev.yawkar.dbms.ElemDBMSManager;
import dev.yawkar.dbms.db.Database;
import dev.yawkar.dbms.db.Table;
import dev.yawkar.dbms.specification.base.Equals;
import dev.yawkar.dbms.specification.base.Greater;
import dev.yawkar.dbms.specification.base.Neg;

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
        studentsTable.getQueriedRows(new Neg(new Equals("name", "yawkar"))).forEach(System.out::println);

        System.out.println();
        studentsTable.getQueriedRows(new Greater("student_id", 1L)).forEach(System.out::println);

        //manager.dropDatabase("file:sample.elmdb");
    }
}
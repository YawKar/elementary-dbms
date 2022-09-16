package dev.yawkar;

import dev.yawkar.dbms.DBManager;
import dev.yawkar.dbms.ElemDBMSManager;
import dev.yawkar.dbms.db.Database;

public class Main {
    public static void main(String[] args) {
        DBManager manager = new ElemDBMSManager();
        Database database = manager.createDatabase("file:sample.elmdb");
        for (var table : database.getTables()) {
            System.out.println(table.getName());
            for (var column : table.getColumns()) {
                System.out.printf("%s(%s) %s\n".formatted(column.getLabel(), column.getIndex(), column.getType()));
            }
        }
        manager.dropDatabase("file:sample.elmdb");
    }
}
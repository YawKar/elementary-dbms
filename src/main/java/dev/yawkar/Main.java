package dev.yawkar;

import dev.yawkar.dbms.DBManager;
import dev.yawkar.dbms.ElemDBMSManager;
import dev.yawkar.dbms.db.Database;
import dev.yawkar.dbms.db.Table;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        DBManager manager = new ElemDBMSManager();
        Database database = manager.getDatabase("file:sample.elmdb");

        List<String> tableNames = database.getTables().stream().map(Table::getName).toList();
        tableNames.forEach(database::dropTable);
        //manager.dropDatabase("file:sample.elmdb");
    }
}
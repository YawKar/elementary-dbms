package dev.yawkar;

import dev.yawkar.dbms.DBManager;
import dev.yawkar.dbms.ElemDBMSManager;
import dev.yawkar.dbms.db.Database;
import dev.yawkar.dbms.db.Table;
import dev.yawkar.dbms.specification.ColumnSpecification;
import dev.yawkar.dbms.specification.TableSpecification;
import dev.yawkar.dbms.specification.definition.ColumnDefinition;
import dev.yawkar.dbms.specification.definition.TableDefinition;

public class Main {
    public static void main(String[] args) {
        DBManager manager = new ElemDBMSManager();
        Database database = manager.getDatabase("file:sample.elmdb");

        TableSpecification tableSpec = new TableDefinition();
        tableSpec.setName("test_table");

        ColumnSpecification pkColumn = new ColumnDefinition();
        pkColumn.setPK(true);
        pkColumn.setLabel("id");
        pkColumn.setType("long");
        tableSpec.addColumn(pkColumn);

        ColumnSpecification nameColumn = new ColumnDefinition();
        nameColumn.setLabel("name");
        nameColumn.setType("string");
        tableSpec.addColumn(nameColumn);

        ColumnSpecification surnameColumn = new ColumnDefinition();
        surnameColumn.setLabel("surname");
        surnameColumn.setType("string");
        tableSpec.addColumn(surnameColumn);

        ColumnSpecification ageColumn = new ColumnDefinition();
        ageColumn.setLabel("age");
        ageColumn.setType("long");
        tableSpec.addColumn(ageColumn);

        Table testTable = database.createTable(tableSpec);

        testTable.insertRow(1, "Vadim", "Karpov", 19);
        testTable.insertRow(0, "Kadim", "Warpov", 91);
        //manager.dropDatabase("file:sample.elmdb");
    }
}
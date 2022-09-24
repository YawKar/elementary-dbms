package dev.yawkar.dbms.db;

import dev.yawkar.dbms.specification.TableSpecification;

import java.util.List;

public interface Database {

    String getUri();
    Table createTable(TableSpecification tableSpecification);
    Table getTable(String tableName);
    List<Table> getTables();
    void dropTable(String tableName);
}

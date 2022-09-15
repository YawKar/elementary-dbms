package dev.yawkar.dbms.db;

import dev.yawkar.dbms.specification.TableSpecification;

public interface Database {

    String getName();
    String getUri();
    Table createTable(TableSpecification tableSpecification);
    Table getTable(String tableName);
    void dropTable(String tableName);
    void dropTable(Table tableName);
}

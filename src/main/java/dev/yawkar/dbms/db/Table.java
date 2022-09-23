package dev.yawkar.dbms.db;

import dev.yawkar.dbms.specification.CriteriaSpecification;
import dev.yawkar.dbms.specification.UpdateSpecification;

import java.util.List;

public interface Table {

    String getName();
    int getTableId();
    List<Column> getColumns();
    List<Row> getRows();
    List<Row> getQueriedRows(CriteriaSpecification criteria);
    void insertRow(Object...values);
    void deleteQueriedRows(CriteriaSpecification criteria);
    void updateQueriedRows(CriteriaSpecification criteria, UpdateSpecification update);
}

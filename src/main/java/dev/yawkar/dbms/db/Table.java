package dev.yawkar.dbms.db;

import dev.yawkar.dbms.specification.CriteriaSpecification;

import java.util.List;

public interface Table {

    String getName();
    List<Column> getColumns();
    List<Row> getRows();
    List<Row> getQueriedRows(CriteriaSpecification criteriaSpecification);
    void insertRow(Object...values);
    void deleteQueriedRows(CriteriaSpecification criteriaSpecification);
}

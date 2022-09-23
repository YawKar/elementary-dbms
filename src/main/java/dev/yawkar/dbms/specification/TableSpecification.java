package dev.yawkar.dbms.specification;

import java.util.List;

public interface TableSpecification {

    String getName();
    void setName(String name);
    List<ColumnSpecification> getColumns();
    ColumnSpecification getColumn(int index);
    ColumnSpecification getColumn(String columnLabel);
    void addColumn(ColumnSpecification column);
    void addColumn(int index, ColumnSpecification column);
    void remove(int index);
    void remove(String columnLabel);
}

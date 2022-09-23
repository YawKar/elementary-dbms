package dev.yawkar.dbms.specification.definition;

import dev.yawkar.dbms.exception.NoSuchColumnLabelException;
import dev.yawkar.dbms.specification.ColumnSpecification;
import dev.yawkar.dbms.specification.TableSpecification;

import java.util.ArrayList;
import java.util.List;

public class TableDefinition implements TableSpecification {

    private String name;
    private final List<ColumnSpecification> columns;

    public TableDefinition() {
        columns = new ArrayList<>();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public List<ColumnSpecification> getColumns() {
        return columns;
    }

    @Override
    public ColumnSpecification getColumn(int index) {
        return columns.get(index);
    }

    @Override
    public ColumnSpecification getColumn(String columnLabel) {
        return columns.stream()
                .filter(c -> c.getLabel().equals(columnLabel))
                .findAny()
                .orElseThrow(() -> new NoSuchColumnLabelException(columnLabel));
    }

    @Override
    public void addColumn(ColumnSpecification column) {
        columns.add(column);
    }

    @Override
    public void addColumn(int index, ColumnSpecification column) {
        columns.add(index, column);
    }

    @Override
    public void remove(int index) {
        columns.remove(index);
    }

    @Override
    public void remove(String columnLabel) {
        columns.removeIf(c -> c.getLabel().equals(columnLabel));
    }
}

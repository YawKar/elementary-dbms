package dev.yawkar.dbms.specification.base;

import dev.yawkar.dbms.db.Row;
import dev.yawkar.dbms.db.Table;
import dev.yawkar.dbms.exception.NoSuchColumnLabelException;
import dev.yawkar.dbms.specification.CriteriaSpecification;

public class Greater implements CriteriaSpecification {

    String columnLabel;
    int columnIndex;
    CriteriaSpecification finalCriteria;

    public Greater(String columnLabel, String stringValue) {
        this.columnLabel = columnLabel;
        finalCriteria = row -> row.get(columnIndex).asString().compareTo(stringValue) > 0;
    }

    public Greater(String columnLabel, Long longValue) {
        this.columnLabel = columnLabel;
        finalCriteria = row -> row.get(columnIndex).asLong() > longValue;
    }

    public Greater(String columnLabel, Double doubleValue) {
        this.columnLabel = columnLabel;
        finalCriteria = row -> row.get(columnIndex).asDouble() > doubleValue;
    }

    @Override
    public boolean examine(Row row) {
        return finalCriteria.examine(row);
    }

    @Override
    public void setup(Table table) {
        columnIndex = table.getColumns().stream()
                .filter(c -> c.getLabel().equals(columnLabel))
                .findAny()
                .orElseThrow(() -> new NoSuchColumnLabelException(columnLabel)).getIndex();
    }
}

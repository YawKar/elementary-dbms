package dev.yawkar.dbms.specification.criteria;

import dev.yawkar.dbms.db.Row;
import dev.yawkar.dbms.db.Table;
import dev.yawkar.dbms.exception.NoSuchColumnLabelException;
import dev.yawkar.dbms.specification.CriteriaSpecification;

public class EQ implements CriteriaSpecification {

    String columnLabel;
    int columnIndex;
    CriteriaSpecification finalCriteria;

    public EQ(String columnLabel, String stringValue) {
        this.columnLabel = columnLabel;
        finalCriteria = row -> row.get(columnIndex).asString().equals(stringValue);
    }

    public EQ(String columnLabel, Long longValue) {
        this.columnLabel = columnLabel;
        finalCriteria = row -> row.get(columnIndex).asLong() == longValue;
    }

    public EQ(String columnLabel, Double doubleValue) {
        this.columnLabel = columnLabel;
        finalCriteria = row -> row.get(columnIndex).asDouble() == doubleValue;
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

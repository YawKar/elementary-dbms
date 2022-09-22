package dev.yawkar.dbms.specification.base.update;

import dev.yawkar.dbms.db.Row;
import dev.yawkar.dbms.db.SimpleRowElement;
import dev.yawkar.dbms.db.Table;
import dev.yawkar.dbms.exception.NoSuchColumnLabelException;
import dev.yawkar.dbms.specification.UpdateSpecification;

public class SET implements UpdateSpecification {

    private final String label;
    private int columnIndex;
    private final String value;

    public SET(String label, String value) {
        this.label = label;
        this.value = value;
    }

    public SET(String label, long value) {
        this(label, Long.toString(value));
    }

    public SET(String label, int value) {
        this(label, Integer.toString(value));
    }

    public SET(String label, double value) {
        this(label, Double.toString(value));
    }

    @Override
    public void update(Row row) {
        row.set(columnIndex, new SimpleRowElement(value));
    }

    @Override
    public void setup(Table table) {
        columnIndex = table.getColumns().stream()
                .filter(c -> c.getLabel().equals(label))
                .findAny()
                .orElseThrow(() -> new NoSuchColumnLabelException(label))
                .getIndex();
    }
}

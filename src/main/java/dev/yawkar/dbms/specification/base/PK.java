package dev.yawkar.dbms.specification.base;

import dev.yawkar.dbms.db.Column;
import dev.yawkar.dbms.db.Row;
import dev.yawkar.dbms.db.Table;
import dev.yawkar.dbms.specification.CriteriaSpecification;

public class PK implements CriteriaSpecification {

    CriteriaSpecification finalCriteria;
    int pKeyColumnIndex;

    public PK(String pKey) {
        finalCriteria = row -> row.get(pKeyColumnIndex).asString().equals(pKey);
    }

    public PK(long pKey) {
        finalCriteria = row -> row.get(pKeyColumnIndex).asLong() == pKey;
    }

    public PK(double pKey) {
        finalCriteria = row -> row.get(pKeyColumnIndex).asDouble() == pKey;
    }

    @Override
    public boolean examine(Row row) {
        return finalCriteria.examine(row);
    }

    @Override
    public void setup(Table table) {
        pKeyColumnIndex = table.getColumns().stream().filter(Column::isPk).findAny().get().getIndex();
    }
}

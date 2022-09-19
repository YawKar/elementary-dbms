package dev.yawkar.dbms.specification.base;

import dev.yawkar.dbms.db.Column;
import dev.yawkar.dbms.db.Row;
import dev.yawkar.dbms.db.Table;
import dev.yawkar.dbms.specification.CriteriaSpecification;

public class ByPK implements CriteriaSpecification {

    CriteriaSpecification finalCriteria;
    int pKeyColumnIndex;

    public ByPK(String pKey) {
        finalCriteria = row -> row.get(pKeyColumnIndex).asString().equals(pKey);
    }

    public ByPK(long pKey) {
        finalCriteria = row -> row.get(pKeyColumnIndex).asLong() == pKey;
    }

    public ByPK(double pKey) {
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

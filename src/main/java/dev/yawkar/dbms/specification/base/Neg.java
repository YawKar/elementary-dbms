package dev.yawkar.dbms.specification.base;

import dev.yawkar.dbms.db.Row;
import dev.yawkar.dbms.db.Table;
import dev.yawkar.dbms.specification.CriteriaSpecification;

public class Neg implements CriteriaSpecification {

    private final CriteriaSpecification innerCriteria;

    public Neg(CriteriaSpecification innerCriteria) {
        this.innerCriteria = innerCriteria;
    }

    @Override
    public boolean examine(Row row) {
        return !innerCriteria.examine(row);
    }

    @Override
    public void setup(Table table) {
        innerCriteria.setup(table);
    }
}

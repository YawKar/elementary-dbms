package dev.yawkar.dbms.specification.criteria;

import dev.yawkar.dbms.db.Row;
import dev.yawkar.dbms.specification.CriteriaSpecification;

public class ALL implements CriteriaSpecification {

    @Override
    public boolean examine(Row row) {
        return true;
    }
}

package dev.yawkar.dbms.specification;

import dev.yawkar.dbms.db.Row;
import dev.yawkar.dbms.db.Table;

public interface CriteriaSpecification {

    boolean examine(Row row);
    default void setup(Table table) {}
}

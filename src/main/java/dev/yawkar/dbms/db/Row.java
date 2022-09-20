package dev.yawkar.dbms.db;

public interface Row {

    RowElement get(int index);
    void set(int index, RowElement rowElement);
}

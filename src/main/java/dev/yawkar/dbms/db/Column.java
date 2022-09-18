package dev.yawkar.dbms.db;

public interface Column {

    String getLabel();
    int getIndex();
    String getType();
    boolean isPk();
}

package dev.yawkar.dbms.specification;

public interface ColumnSpecification {

    String getLabel();
    void setLabel(String label);

    boolean isPK();
    void setPK(boolean pk);

    String getType();
    void setType(String type);
}

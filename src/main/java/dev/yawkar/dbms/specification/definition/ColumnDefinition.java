package dev.yawkar.dbms.specification.definition;

import dev.yawkar.dbms.specification.ColumnSpecification;

public class ColumnDefinition implements ColumnSpecification {

    private String label;
    private String type;
    private boolean pk;

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public boolean isPK() {
        return pk;
    }

    @Override
    public void setPK(boolean pk) {
        this.pk = pk;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void setType(String type) {
        this.type = type;
    }
}

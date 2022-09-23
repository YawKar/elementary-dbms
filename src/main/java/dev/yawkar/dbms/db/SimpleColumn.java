package dev.yawkar.dbms.db;

public class SimpleColumn implements Column {

    String label;
    int index;
    String type;
    boolean pk;

    SimpleColumn() {}

    SimpleColumn(String label, int index, String type) {
        this.label = label;
        this.index = index;
        this.type = type;
    }

    SimpleColumn(String label, int index, String type, boolean pk) {
        this(label, index, type);
        this.pk = pk;
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public boolean isPk() {
        return pk;
    }
}

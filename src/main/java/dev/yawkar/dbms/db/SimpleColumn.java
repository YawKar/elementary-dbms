package dev.yawkar.dbms.db;

public class SimpleColumn implements Column {

    String label;
    int index;
    String type;

    SimpleColumn() {}

    SimpleColumn(String label, int index, String type) {
        this.label = label;
        this.index = index;
        this.type = type;
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
}

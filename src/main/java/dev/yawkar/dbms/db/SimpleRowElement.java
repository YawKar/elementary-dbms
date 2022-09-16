package dev.yawkar.dbms.db;

public class SimpleRowElement implements RowElement {

    String content;

    SimpleRowElement() {}

    SimpleRowElement(String content) {
        this.content = content;
    }

    @Override
    public String asString() {
        return content;
    }

    @Override
    public long asLong() {
        return Long.parseLong(content);
    }

    @Override
    public double asDouble() {
        return Double.parseDouble(content);
    }
}

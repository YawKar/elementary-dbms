package dev.yawkar.dbms.db;

import java.util.ArrayList;
import java.util.List;

public class SimpleRow implements Row {

    List<RowElement> elements = new ArrayList<>();

    @Override
    public RowElement get(int index) {
        return elements.get(index);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (var element : elements) {
            sb.append(element.asString()).append(' ');
        }
        return sb.toString();
    }
}

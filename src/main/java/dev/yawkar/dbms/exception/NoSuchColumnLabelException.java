package dev.yawkar.dbms.exception;

public class NoSuchColumnLabelException extends ElemDBMSException {

    public NoSuchColumnLabelException() {}

    public NoSuchColumnLabelException(String message) {
        super(message);
    }
}

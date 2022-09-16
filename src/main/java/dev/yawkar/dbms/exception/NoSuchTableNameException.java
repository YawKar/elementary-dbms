package dev.yawkar.dbms.exception;

public class NoSuchTableNameException extends ElemDBMSException {

    public NoSuchTableNameException() {}

    public NoSuchTableNameException(String message) {
        super(message);
    }
}

package dev.yawkar.dbms.exception;

public class TableWithSuchNameAlreadyExistsException extends ElemDBMSException {

    public TableWithSuchNameAlreadyExistsException() {}

    public TableWithSuchNameAlreadyExistsException(String message) {
        super(message);
    }
}

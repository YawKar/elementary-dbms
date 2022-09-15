package dev.yawkar.dbms.exception;

public class DatabaseFileAlreadyExistsException extends ElemDBMSException {

    public DatabaseFileAlreadyExistsException() {}

    public DatabaseFileAlreadyExistsException(String message) {
        super(message);
    }
}

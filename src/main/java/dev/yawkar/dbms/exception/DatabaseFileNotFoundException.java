package dev.yawkar.dbms.exception;

public class DatabaseFileNotFoundException extends ElemDBMSException {

    public DatabaseFileNotFoundException() {}

    public DatabaseFileNotFoundException(String message) {
        super(message);
    }
}

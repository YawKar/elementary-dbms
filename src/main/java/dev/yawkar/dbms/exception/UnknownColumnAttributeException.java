package dev.yawkar.dbms.exception;

public class UnknownColumnAttributeException extends ElemDBMSException {

    public UnknownColumnAttributeException() {}

    public UnknownColumnAttributeException(String message) {
        super(message);
    }
}

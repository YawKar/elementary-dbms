package dev.yawkar.dbms.exception;

public class CannotPerformOperationException extends ElemDBMSException {

    public CannotPerformOperationException() {}

    public CannotPerformOperationException(String message) {
        super(message);
    }
}

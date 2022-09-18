package dev.yawkar.dbms.exception;

public class PrimaryKeyConstraintViolationException extends ElemDBMSException {

    public PrimaryKeyConstraintViolationException() {}

    public PrimaryKeyConstraintViolationException(String message) {
        super(message);
    }
}

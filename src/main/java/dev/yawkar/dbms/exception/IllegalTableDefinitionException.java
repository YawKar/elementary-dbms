package dev.yawkar.dbms.exception;

public class IllegalTableDefinitionException extends ElemDBMSException {

    public IllegalTableDefinitionException() {}

    public IllegalTableDefinitionException(String message) {
        super(message);
    }
}

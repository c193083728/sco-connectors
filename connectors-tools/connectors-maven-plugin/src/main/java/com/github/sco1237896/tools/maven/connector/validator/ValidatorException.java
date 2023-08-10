package com.github.sco1237896.tools.maven.connector.validator;

public class ValidatorException extends RuntimeException {
    public ValidatorException() {
    }

    public ValidatorException(String message) {
        super(message);
    }

    public ValidatorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ValidatorException(Throwable cause) {
        super(cause);
    }
}

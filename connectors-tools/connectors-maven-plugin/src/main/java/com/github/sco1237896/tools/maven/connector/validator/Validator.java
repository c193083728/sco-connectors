package com.github.sco1237896.tools.maven.connector.validator;

import java.nio.file.Path;

import com.github.sco1237896.tools.maven.connector.support.Connector;
import org.apache.maven.plugin.logging.Log;

import com.fasterxml.jackson.databind.node.ObjectNode;

public interface Validator {
    void validate(Context context, ObjectNode definition);

    enum Mode {
        WARN,
        FAIL
    }

    interface Context {
        Path getCatalogPath();

        Connector getConnector();

        Log getLog();

        Mode getMode();
    }
}

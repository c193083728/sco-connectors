package com.github.sco1237896.connector.core;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;

@ConfigRoot(name = "connector", phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED)
public class ConnectorConfig {
}

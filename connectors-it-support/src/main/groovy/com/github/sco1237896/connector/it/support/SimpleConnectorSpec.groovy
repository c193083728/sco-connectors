package com.github.sco1237896.connector.it.support

import groovy.util.logging.Slf4j
import io.restassured.RestAssured
import io.restassured.builder.RequestSpecBuilder
import io.restassured.http.ContentType

@Slf4j
abstract class SimpleConnectorSpec extends ConnectorSpecSupport {
    def setupSpec() {
        RestAssured.requestSpecification = new RequestSpecBuilder()
            .setContentType(ContentType.JSON)
            .setAccept(ContentType.JSON)
            .build()
    }
}

#!/bin/bash

gfsh <<!

start locator --name=locator --properties-file=../config/locator.properties

start server --name=server1 --cache-xml-file=../config/cache.xml --properties-file=../config/gemfire.properties --classpath=../../target/unit-telemetry-expiration-0.0.1-SNAPSHOT.jar --J=-Dgemfire.start-dev-rest-api=true --J=-Dgemfire.http-service-port=7071

start server --name=server2 --cache-xml-file=../config/cache.xml --properties-file=../config/gemfire.properties --classpath=../../target/unit-telemetry-expiration-0.0.1-SNAPSHOT.jar --server-port=40405

start server --name=server3 --cache-xml-file=../config/cache.xml --properties-file=../config/gemfire.properties --classpath=../../target/unit-telemetry-expiration-0.0.1-SNAPSHOT.jar --server-port=40406

list members;

list regions;

exit;
!




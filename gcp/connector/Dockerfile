FROM --platform=linux/amd64 public.ecr.aws/lambda/java:21

COPY athena-gcs-2022.47.1.jar ${LAMBDA_TASK_ROOT}/lib/
CMD ["com.amazonaws.athena.connectors.gcs.GcsCompositeHandler::handleRequest"]
ENV JAVA_TOOL_OPTIONS="--add-opens=java.base/java.nio=ALL-UNNAMED"



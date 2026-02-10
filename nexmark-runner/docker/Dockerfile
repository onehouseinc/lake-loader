ARG FLINK_VERSION=1.18.1
FROM flink:${FLINK_VERSION}-java11

# Build arguments
# Note: ARGs before FROM go out of scope after FROM, so FLINK_VERSION must be re-declared
ARG FLINK_VERSION=1.18.1
# NEXMARK_CONFIG_FILE: Config for JAR selection and runtime sink DDL
# Empty/unset = no additional JARs (blackhole sink uses nexmark defaults)
ARG NEXMARK_CONFIG_FILE=
ARG HUDI_VERSION=1.1.1
ARG SIDE_INPUT_NUM=10000

ENV FLINK_VERSION=${FLINK_VERSION}
ENV HUDI_VERSION=${HUDI_VERSION}

USER root

# Install JDK, git, Maven, gettext (for envsubst), yq (base image only has JRE)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk-headless git maven gettext-base && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/lib/jvm/java-11-openjdk-* /usr/lib/jvm/java-11-openjdk && \
    wget -q -O /usr/local/bin/yq https://github.com/mikefarah/yq/releases/download/v4.40.5/yq_linux_amd64 && \
    chmod +x /usr/local/bin/yq

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk

# Download Flink plugins and connectors (single layer)
# - GCS/S3 filesystem plugins for Flink
# - Hadoop bundle + AWS SDK for Hudi S3/MinIO support
# - GCS connector for Hudi GCS support
RUN mkdir -p /opt/flink/plugins/gs-fs-hadoop /opt/flink/plugins/s3-fs-hadoop && \
    wget -q -P /opt/flink/plugins/gs-fs-hadoop \
        https://repo1.maven.org/maven2/org/apache/flink/flink-gs-fs-hadoop/${FLINK_VERSION}/flink-gs-fs-hadoop-${FLINK_VERSION}.jar && \
    wget -q -P /opt/flink/plugins/s3-fs-hadoop \
        https://repo1.maven.org/maven2/org/apache/flink/flink-s3-fs-hadoop/${FLINK_VERSION}/flink-s3-fs-hadoop-${FLINK_VERSION}.jar && \
    wget -q -P /opt/flink/lib \
        https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar && \
    wget -q -P /opt/flink/lib \
        https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.2.18/gcs-connector-hadoop2-2.2.18-shaded.jar && \
    wget -q -P /opt/flink/lib \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.8.5/hadoop-aws-2.8.5.jar && \
    wget -q -P /opt/flink/lib \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar && \
    wget -q -P /opt/flink/lib \
        https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar && \
    echo "Downloaded all Flink plugins and connectors"

# Replace planner-loader with full flink-table-planner
# Removes loader to avoid "Multiple factories for identifier 'default'" conflict
RUN rm -f /opt/flink/lib/flink-table-planner-loader-*.jar && \
    cp /opt/flink/opt/flink-table-planner_2.12-${FLINK_VERSION}.jar /opt/flink/lib/

# Copy and build Nexmark from local directory (with pluggable sink support)
COPY ../nexmark /tmp/nexmark
RUN cd /tmp/nexmark && \
    mvn clean package -pl nexmark-flink -am -Dmaven.test.skip=true && \
    cp -r nexmark-flink/target/nexmark-flink-bin/nexmark-flink /opt/nexmark && \
    cp /opt/nexmark/lib/nexmark-flink*.jar /opt/flink/lib/ && \
    rm -rf /tmp/nexmark ~/.m2

# Copy JARs specified in config's include_jars list
# Supports two formats:
#   1. JAR filename (ending with .jar): copied from docker/jars/
#   2. Maven coordinates (groupId:artifactId:version): downloaded from Maven Central
# Variables like ${FLINK_VERSION_SHORT}, ${HUDI_VERSION} are resolved against env vars
# If NEXMARK_CONFIG_FILE is empty, no additional JARs are included (blackhole mode)
COPY nexmark-runner/docker/jars/ /tmp/build-jars/
COPY nexmark-runner/configs/ /tmp/build-configs/
RUN set -e && \
    FLINK_VERSION_SHORT=$(echo "$FLINK_VERSION" | cut -d. -f1,2) && \
    export FLINK_VERSION_SHORT FLINK_VERSION HUDI_VERSION && \
    if [ -z "$NEXMARK_CONFIG_FILE" ]; then \
        echo "No NEXMARK_CONFIG_FILE specified (blackhole mode - no additional JARs)"; \
    elif [ -f "/tmp/build-configs/$NEXMARK_CONFIG_FILE" ]; then \
        CONFIG_PATH="/tmp/build-configs/$NEXMARK_CONFIG_FILE"; \
        if yq -e '.include_jars' "$CONFIG_PATH" > /dev/null 2>&1; then \
            echo "=== Processing include_jars from $NEXMARK_CONFIG_FILE ==="; \
            yq -r '.include_jars[]' "$CONFIG_PATH" > /tmp/jar_entries.txt; \
            while IFS= read -r entry || [ -n "$entry" ]; do \
                resolved=$(echo "$entry" | envsubst); \
                echo "  Processing: $entry -> $resolved"; \
                if echo "$resolved" | grep -q '\.jar$'; then \
                    if [ -f "/tmp/build-jars/$resolved" ]; then \
                        cp "/tmp/build-jars/$resolved" /opt/flink/lib/; \
                        echo "    Copied: $resolved"; \
                    else \
                        echo "ERROR: JAR not found: $resolved"; \
                        echo "Available JARs in docker/jars/:"; \
                        ls -la /tmp/build-jars/ || echo "  (empty)"; \
                        exit 1; \
                    fi; \
                else \
                    GROUP_ID=$(echo "$resolved" | cut -d: -f1); \
                    ARTIFACT_ID=$(echo "$resolved" | cut -d: -f2); \
                    VERSION=$(echo "$resolved" | cut -d: -f3); \
                    if [ -z "$GROUP_ID" ] || [ -z "$ARTIFACT_ID" ] || [ -z "$VERSION" ]; then \
                        echo "ERROR: Invalid Maven coordinate: $resolved"; \
                        echo "Expected format: groupId:artifactId:version"; \
                        exit 1; \
                    fi; \
                    GROUP_PATH=$(echo "$GROUP_ID" | tr '.' '/'); \
                    JAR_NAME="${ARTIFACT_ID}-${VERSION}.jar"; \
                    MAVEN_URL="https://repo1.maven.org/maven2/${GROUP_PATH}/${ARTIFACT_ID}/${VERSION}/${JAR_NAME}"; \
                    echo "    Downloading from Maven Central: $MAVEN_URL"; \
                    wget -q -P /opt/flink/lib "$MAVEN_URL"; \
                    echo "    Downloaded: $JAR_NAME"; \
                fi; \
            done < /tmp/jar_entries.txt; \
            rm -f /tmp/jar_entries.txt; \
        else \
            echo "No include_jars in $NEXMARK_CONFIG_FILE"; \
        fi; \
    else \
        echo "ERROR: Config file not found: $NEXMARK_CONFIG_FILE"; \
        exit 1; \
    fi && \
    rm -rf /tmp/build-jars /tmp/build-configs

# Create nexmark config directory (nexmark.yaml provided at runtime)
# K8s: ConfigMap mounts nexmark.yaml directly to /opt/nexmark/conf/
# YARN: ship nexmark.yaml via -files, set NEXMARK_CONF_DIR to point to it
RUN mkdir -p /opt/nexmark/conf

# Generate side input data for q13 (bounded side input join)
ARG SIDE_INPUT_NUM
RUN mkdir -p /opt/flink/data && \
    java -cp "/opt/nexmark/lib/*:/opt/flink/lib/*" \
        com.github.nexmark.flink.generator.SideInputGenerator \
        --num ${SIDE_INPUT_NUM} --path /opt/flink/data/side_input.txt

# Set environment variables
ENV FLINK_HOME=/opt/flink
ENV NEXMARK_HOME=/opt/nexmark
ENV NEXMARK_CONF_DIR=/opt/nexmark/conf

# Create directories and set permissions
RUN mkdir -p /opt/flink/usrlib && \
    chown -R flink:flink /opt/flink /opt/nexmark

# Verify setup
RUN echo "=== Nexmark Package ===" && \
    ls -la /opt/nexmark/ && \
    echo "=== Nexmark Config Dir (runtime mount) ===" && \
    ls -la /opt/nexmark/conf/ && \
    echo "=== Flink Lib JARs ===" && \
    ls -la /opt/flink/lib/*.jar && \
    echo "=== Flink Filesystem Plugins ===" && \
    ls -la /opt/flink/plugins/*/

USER flink

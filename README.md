# Marine Data Instrumentation
Instrumentation of data metrics using [Prometheus JVM client library](https://github.com/prometheus/client_java).

## Docker Build and Execution

```bash
docker build -t data_instrumentation .
docker run -p 9091:9091 data_instrumentation:latest
```

To customize configuration parameters pass the absolut path of the YAML configuration file (*$PATH_TO_YAML*) as argument in the docker command:

```bash
docker run -p 9091:9091 -v $PATH_TO_YAML:/etc/data_instrumentation/config.yaml data_instrumentation:latest
```

## Metrics

*Namespace* - WIP

*Lables* - WIP

*Categories* - Data Metrics, Service Metrics (FaaS), System Metrics

### Conventions
Tries to follow conventions from OpenTelemetry and OpenMetrics Initiatives:

- [OpenTelemetry](https://github.com/open-telemetry/opentelemetry-specification/tree/main/specification/metrics/semantic_conventions#metrics-semantic-conventions)

- [OpenMetrics](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md)

- [OpenTelemetry sinergies with OpenMetrics and Prometheus](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/compatibility/prometheus_and_openmetrics.md)

### Data Metrics

### System Metrics

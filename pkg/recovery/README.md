# HED Recovery

The recovery package provides bounded dependency recovery and an adaptive telemetry anomaly detector.

Recovery is deliberately idempotent and never creates or replays transactions. The sequence is:

1. Drain traffic.
2. Reconnect storage.
3. Reconnect Fabric.
4. Resume traffic.

Retries are bounded by a total recovery timeout and maximum attempts. Circuit breaking prevents repeated calls while a dependency is unhealthy.

`AnomalyDetector` uses EWMA statistics and an absolute z-score threshold. It is adaptive anomaly detection, not a trained AI model.

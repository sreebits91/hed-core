package recovery

import "testing"

func TestAnomalyDetectorFlagsLargeDeviation(t *testing.T) {
	d := NewAnomalyDetector(0.1, 4)
	for i := 0; i < 100; i++ { d.Observe(100) }
	anomaly, score := d.Observe(1000)
	if !anomaly { t.Fatalf("expected anomaly, z=%v", score) }
}

func TestAnomalyDetectorHandlesInvalidTelemetry(t *testing.T) {
	d := NewAnomalyDetector(0.1, 4)
	anomaly, _ := d.Observe(0.0 / 0.0)
	if !anomaly { t.Fatal("NaN telemetry must be anomalous") }
}

package recovery

import (
	"math"
	"sync"
)

// AnomalyDetector is an online, explainable detector for HED telemetry.
// It is deliberately not presented as a trained AI model: it uses EWMA mean
// and variance so the production system can react without a model artifact.
type AnomalyDetector struct {
	mu     float64
	varEW  float64
	alpha  float64
	count  uint64
	limit  float64
	muLock sync.Mutex
}

func NewAnomalyDetector(alpha, zLimit float64) *AnomalyDetector {
	if alpha <= 0 || alpha >= 1 { alpha = 0.1 }
	if zLimit <= 0 { zLimit = 4 }
	return &AnomalyDetector{alpha: alpha, limit: zLimit}
}

// Observe returns whether x is anomalous and its absolute z-score.
func (d *AnomalyDetector) Observe(x float64) (bool, float64) {
	d.muLock.Lock()
	defer d.muLock.Unlock()
	if math.IsNaN(x) || math.IsInf(x, 0) { return true, math.Inf(1) }
	d.count++
	if d.count == 1 { d.mu = x; return false, 0 }
	delta := x - d.mu
	d.mu += d.alpha * delta
	d.varEW = (1-d.alpha) * (d.varEW + d.alpha*delta*delta)
	std := math.Sqrt(math.Max(d.varEW, 1e-12))
	z := math.Abs(x-d.mu) / std
	return z >= d.limit, z
}

func (d *AnomalyDetector) Mean() float64 { d.muLock.Lock(); defer d.muLock.Unlock(); return d.mu }
func (d *AnomalyDetector) Variance() float64 { d.muLock.Lock(); defer d.muLock.Unlock(); return d.varEW }

package gate

import (
	"math"
	"slices"
)

// Point is one sample of a series.
type Point struct {
	// T is the sample time in seconds since the cell epoch.
	T float64
	// V is the sampled value.
	V float64
}

// Series is a time-ordered list of samples.
type Series []Point

// TheilSenSlope estimates the slope of a series per second.
//
// It is the median of the slopes of every pair of points with distinct times,
// so a single outlier cannot move it, while a sustained rise or step does.
// Points whose value is NaN or infinite are skipped: missing is never zero.
//
// Parameters:
//   - s: the series; the order of its points does not matter
//
// Returns:
//   - float64: the slope in value units per second
//   - bool: false when fewer than one usable pair exists
func TheilSenSlope(s Series) (float64, bool) {
	pts := make(Series, 0, len(s))
	for _, p := range s {
		if isFinite(p.V) && isFinite(p.T) {
			pts = append(pts, p)
		}
	}
	slopes := make([]float64, 0, len(pts)*(len(pts)-1)/2)
	for i := range pts {
		for j := i + 1; j < len(pts); j++ {
			dt := pts[j].T - pts[i].T
			if dt == 0 {
				continue
			}
			slopes = append(slopes, (pts[j].V-pts[i].V)/dt)
		}
	}
	return Median(slopes)
}

// Median returns the median of vals.
//
// Parameters:
//   - vals: the values; not modified
//
// Returns:
//   - float64: the median, the mean of the middle two for an even count
//   - bool: false when vals is empty
func Median(vals []float64) (float64, bool) {
	if len(vals) == 0 {
		return 0, false
	}
	sorted := slices.Clone(vals)
	slices.Sort(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid], true
	}
	return (sorted[mid-1] + sorted[mid]) / 2, true
}

// Percentile returns the nearest-rank percentile q of vals.
//
// Parameters:
//   - vals: the values; not modified
//   - q: the quantile in (0, 1]
//
// Returns:
//   - float64: the smallest value with at least q of the values at or below it
//   - bool: false when vals is empty
func Percentile(vals []float64, q float64) (float64, bool) {
	if len(vals) == 0 {
		return 0, false
	}
	sorted := slices.Clone(vals)
	slices.Sort(sorted)
	rank := int(math.Ceil(q * float64(len(sorted))))
	rank = min(max(rank, 1), len(sorted))
	return sorted[rank-1], true
}

// Window returns the points with from <= T <= to.
//
// Parameters:
//   - from: the first time included, in seconds
//   - to: the last time included, in seconds
//
// Returns:
//   - Series: a new series holding the points in the window
func (s Series) Window(from, to float64) Series {
	var out Series
	for _, p := range s {
		if p.T >= from && p.T <= to {
			out = append(out, p)
		}
	}
	return out
}

// Values returns the values of the series in order.
//
// Returns:
//   - []float64: one value per point
func (s Series) Values() []float64 {
	out := make([]float64, len(s))
	for i, p := range s {
		out[i] = p.V
	}
	return out
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

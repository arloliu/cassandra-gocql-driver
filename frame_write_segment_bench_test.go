package gocql

import (
	"testing"
)

// buildTChartBatchReq models the FDC offline "TCHART" batch write measured in the
// data-collection profile: a single prepared INSERT replayed over N rows, 8 columns
// each (eqp_id, source, svid_name, date text; dc_dt bigint; val double; data text;
// tag text). Every statement shares one preparedID (the fast-path shape). The values
// are pre-marshalled so the benchmark isolates frame serialization + proto-v5 segment
// framing, not marshalQueryValue.
func buildTChartBatchReq(rows int) *writeBatchFrame {
	preparedID := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB}

	// Representative marshalled column widths for one TCHART row (~66 payload bytes).
	mkRow := func(i int) []queryValues {
		return []queryValues{
			{value: []byte("TOOL_00123")},                                   // eqp_id text
			{value: []byte("SECS")},                                         // source text
			{value: []byte("PRESSURE_CHAMBER_A_SETPOINT")},                  // svid_name text
			{value: []byte("2026-07-10")},                                   // date text
			{value: []byte{0, 0, 1, 0x92, 0x3f, 0x1a, 0x2b, byte(i)}},       // dc_dt bigint (8)
			{value: []byte{0x40, 0x09, 0x21, 0xfb, 0x54, 0x44, 0x2d, 0x18}}, // val double (8)
			{value: []byte("")},                                             // data text (empty, common)
			{value: []byte("OK")},                                           // tag text
		}
	}

	stmts := make([]batchStatment, rows)
	for i := range stmts {
		stmts[i] = batchStatment{
			preparedID: preparedID,
			values:     mkRow(i),
		}
	}

	return &writeBatchFrame{
		typ:         UnloggedBatch,
		statements:  stmts,
		consistency: Quorum,
	}
}

// BenchmarkWriteBatchFrameSegment measures the write-side cost of serializing a
// proto-v5 batch frame and applying the modern (segmented) layout, using the pooled
// framer exactly as execInternal does. This is the residual write path after B2
// (framer buffer retention) and B4 (batch collection pooling): any allocs/op here are
// the un-pooled proto-v5 write-side segment framing (newUncompressedSegment make +
// prepareModernLayout append).
func BenchmarkWriteBatchFrameSegment(b *testing.B) {
	sizes := []int{1, 10, 100}
	for _, n := range sizes {
		req := buildTChartBatchReq(n)
		b.Run(rowsLabel(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				f := getFramer(nil, protoVersion5, GlobalTypes)
				if err := f.writeBatchFrame(0, req, nil); err != nil {
					b.Fatal(err)
				}
				if err := f.prepareModernLayout(); err != nil {
					b.Fatal(err)
				}
				f.release()
			}
		})
	}
}

func rowsLabel(n int) string {
	switch n {
	case 1:
		return "rows1"
	case 10:
		return "rows10"
	case 100:
		return "rows100"
	default:
		return "rowsN"
	}
}

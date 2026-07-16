package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
)

// safetensorsHeader is the JSON header of a .safetensors file: a map from
// tensor name to its dtype/shape/byte-offsets within the trailing raw data
// blob. See https://github.com/huggingface/safetensors for the format.
type safetensorsHeader map[string]safetensorsTensorInfo

type safetensorsTensorInfo struct {
	Dtype       string `json:"dtype"`
	Shape       []int  `json:"shape"`
	DataOffsets [2]int `json:"data_offsets"`
}

// loadSafetensorsMatrix reads a single named tensor out of a .safetensors
// file and returns it widened to row-major []float64 plus its shape. Two
// dtypes are supported, matching the two forms the model2vec embedding
// matrix ships in:
//
//   - F32: each 4-byte little-endian value decoded as float32, widened to
//     float64.
//   - I8: each signed byte widened to float64 AS-IS, with no dequantization
//     scale applied. This matches model2vec's own int8 encode path: the
//     package's int8 quantization uses a single per-tensor symmetric scale
//     (max|W|/127) that cancels out under L2 normalization, so the saved
//     int8 artifact carries no scale tensor at all — Python's StaticModel
//     pools the raw int8 values as float (numpy mean promotes int8->float64)
//     and normalizes, with no rescale step. Widening here without scaling is
//     therefore not an approximation; it is the same code path the real
//     package runs.
//
// Format: 8-byte little-endian header length, then that many bytes of JSON
// header, then the raw tensor bytes (row-major, little-endian for F32) at
// the offsets the header declares (relative to the end of the header).
func loadSafetensorsMatrix(path, tensorName string) (data []float64, shape []int, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open safetensors %s: %w", path, err)
	}
	defer f.Close()

	var lenBuf [8]byte
	if _, err := readFull(f, lenBuf[:]); err != nil {
		return nil, nil, fmt.Errorf("read safetensors header length: %w", err)
	}
	headerLen := binary.LittleEndian.Uint64(lenBuf[:])
	// Sanity bound: a header over 64MiB is not a real model2vec file and would
	// otherwise trigger an enormous allocation from a truncated/corrupt file.
	const maxHeaderLen = 64 << 20
	if headerLen == 0 || headerLen > maxHeaderLen {
		return nil, nil, fmt.Errorf("safetensors header length %d out of bounds", headerLen)
	}

	headerBytes := make([]byte, headerLen)
	if _, err := readFull(f, headerBytes); err != nil {
		return nil, nil, fmt.Errorf("read safetensors header: %w", err)
	}

	var header safetensorsHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, nil, fmt.Errorf("parse safetensors header: %w", err)
	}

	info, ok := header[tensorName]
	if !ok {
		return nil, nil, fmt.Errorf("safetensors file has no tensor %q", tensorName)
	}

	var elemSize int
	switch info.Dtype {
	case "F32":
		elemSize = 4
	case "I8":
		elemSize = 1
	default:
		return nil, nil, fmt.Errorf("tensor %q has dtype %q, want F32 or I8", tensorName, info.Dtype)
	}

	nElems := 1
	for _, d := range info.Shape {
		if d <= 0 {
			return nil, nil, fmt.Errorf("tensor %q has invalid shape %v", tensorName, info.Shape)
		}
		nElems *= d
	}
	wantBytes := nElems * elemSize
	gotBytes := info.DataOffsets[1] - info.DataOffsets[0]
	if gotBytes != wantBytes {
		return nil, nil, fmt.Errorf("tensor %q data_offsets span %d bytes, want %d for shape %v",
			tensorName, gotBytes, wantBytes, info.Shape)
	}

	// data_offsets are relative to the start of the raw data blob, which
	// immediately follows the header.
	if _, err := f.Seek(int64(8+headerLen)+int64(info.DataOffsets[0]), 0); err != nil {
		return nil, nil, fmt.Errorf("seek to tensor %q data: %w", tensorName, err)
	}

	raw := make([]byte, wantBytes)
	if _, err := readFull(f, raw); err != nil {
		return nil, nil, fmt.Errorf("read tensor %q data: %w", tensorName, err)
	}

	out := make([]float64, nElems)
	switch info.Dtype {
	case "F32":
		for i := range out {
			bits := binary.LittleEndian.Uint32(raw[i*4 : i*4+4])
			out[i] = float64(math.Float32frombits(bits))
		}
	case "I8":
		for i := range out {
			// raw[i] is the two's-complement byte; int8() reinterprets it as
			// signed, matching numpy's int8 dtype. No dequant scale — see the
			// doc comment above.
			out[i] = float64(int8(raw[i]))
		}
	}
	return out, info.Shape, nil
}

// readFull reads exactly len(buf) bytes from f, treating a short read as an
// error (os.File.Read can return less than requested even without EOF).
func readFull(f *os.File, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := f.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, fmt.Errorf("read returned 0 bytes with no error")
		}
	}
	return total, nil
}

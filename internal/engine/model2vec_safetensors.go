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

// loadSafetensorsF32 reads a single named F32 tensor out of a .safetensors
// file and returns it as a row-major []float32 plus its shape.
//
// Format: 8-byte little-endian header length, then that many bytes of JSON
// header, then the raw tensor bytes (row-major, little-endian) at the
// offsets the header declares (relative to the end of the header).
func loadSafetensorsF32(path, tensorName string) (data []float32, shape []int, err error) {
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
	if info.Dtype != "F32" {
		return nil, nil, fmt.Errorf("tensor %q has dtype %q, want F32", tensorName, info.Dtype)
	}

	nElems := 1
	for _, d := range info.Shape {
		if d <= 0 {
			return nil, nil, fmt.Errorf("tensor %q has invalid shape %v", tensorName, info.Shape)
		}
		nElems *= d
	}
	wantBytes := nElems * 4
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

	out := make([]float32, nElems)
	for i := range out {
		bits := binary.LittleEndian.Uint32(raw[i*4 : i*4+4])
		out[i] = math.Float32frombits(bits)
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

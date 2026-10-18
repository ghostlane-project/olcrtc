package vp8

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ErrNoReference is returned when an inter-frame arrives before any keyframe.
// Callers should skip the frame and wait for a keyframe.
var ErrNoReference = errors.New("vp8: inter-frame without reference")

// Decoder decodes VP8 frames (keyframes and inter-frames).
type Decoder struct {
	// Reference frames for inter prediction.
	last   *Frame
	golden *Frame
	altref *Frame

	// Sub-block mode context for B_PRED (RFC 6386 section 11.3).
	bModeAboveRow [][4]int // bottom-row modes per MB column
	bModeLeft     [4]int   // right-column modes of MB to the left

	// NNZ (non-zero count) tracking for token coefficient context.
	topNNZY  []bool // 4 entries per MB column
	topNNZU  []bool // 2 entries per MB column
	topNNZV  []bool // 2 entries per MB column
	topNNZY2 []bool // 1 entry per MB column
	leftNNZY [4]bool
	leftNNZU [2]bool
	leftNNZV [2]bool
	leftY2   bool
}

// Frame holds decoded frame data.
type Frame struct {
	Width, Height int
	Y             []byte // luma plane, padded stride
	U, V          []byte // chroma planes
	yStride       int
	uvStride      int
}

// NewDecoder creates a new VP8 decoder.
func NewDecoder() *Decoder {
	return &Decoder{}
}

// Grayscale extracts the Y plane as a width*height grayscale image.
func (f *Frame) Grayscale() []byte {
	out := make([]byte, f.Width*f.Height)
	for y := 0; y < f.Height; y++ {
		copy(out[y*f.Width:(y+1)*f.Width], f.Y[y*f.yStride:y*f.yStride+f.Width])
	}
	return out
}

// Decode decodes a raw VP8 frame.
//
// VP8 frame decoding is inherently complex per RFC 6386.
func (d *Decoder) Decode(data []byte) (*Frame, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("vp8: frame too short")
	}

	// Uncompressed frame tag (3 bytes).
	tag := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
	keyframe := (tag & 1) == 0
	firstPartSize := tag >> 5

	var width, height int
	var dataOffset int

	if keyframe {
		if data[3] != 0x9D || data[4] != 0x01 || data[5] != 0x2A {
			return nil, fmt.Errorf("vp8: invalid start code")
		}
		width = int(binary.LittleEndian.Uint16(data[6:8])) & 0x3FFF
		height = int(binary.LittleEndian.Uint16(data[8:10])) & 0x3FFF
		dataOffset = 10
	} else {
		if d.last == nil {
			return nil, ErrNoReference
		}
		width = d.last.Width
		height = d.last.Height
		dataOffset = 3
	}

	mbW := (width + 15) >> 4
	mbH := (height + 15) >> 4
	yStride := mbW * 16
	uvStride := mbW * 8

	fpEnd := dataOffset + int(firstPartSize)
	if fpEnd > len(data) {
		return nil, fmt.Errorf("vp8: first partition truncated")
	}

	br := newBoolReader(data[dataOffset:fpEnd])

	// Parse header.
	fh, err := d.parseHeader(br, keyframe)
	if err != nil {
		return nil, err
	}

	// Allocate frame.
	frame := &Frame{
		Width:    width,
		Height:   height,
		Y:        make([]byte, yStride*mbH*16),
		U:        make([]byte, uvStride*mbH*8),
		V:        make([]byte, uvStride*mbH*8),
		yStride:  yStride,
		uvStride: uvStride,
	}

	// Token partition.
	tbr := newBoolReader(data[fpEnd:])

	// Decode macroblocks.
	d.bModeAboveRow = make([][4]int, mbW)
	d.topNNZY = make([]bool, 4*mbW)
	d.topNNZU = make([]bool, 2*mbW)
	d.topNNZV = make([]bool, 2*mbW)
	d.topNNZY2 = make([]bool, mbW)
	for mbY := 0; mbY < mbH; mbY++ {
		d.bModeLeft = [4]int{}
		d.leftNNZY = [4]bool{}
		d.leftNNZU = [2]bool{}
		d.leftNNZV = [2]bool{}
		d.leftY2 = false
		for mbX := 0; mbX < mbW; mbX++ {
			if err := d.decodeMB(br, tbr, frame, mbX, mbY, &fh, keyframe); err != nil {
				return nil, fmt.Errorf("vp8: mb[%d,%d]: %w", mbX, mbY, err)
			}
		}
	}

	// Loop filter.
	loopFilter(frame, fh.filterLevel, fh.sharpness, mbW, mbH)

	// Update reference frames.
	if keyframe {
		d.last = frame
		d.golden = frame
		d.altref = frame
	} else {
		d.last = frame
		// TODO: handle golden/altref refresh flags.
	}

	return frame, nil
}

type frameHeader struct {
	coeffProbs  [4][8][3][11]uint8
	yACQI       int
	yDCDelta    int
	y2DCDelta   int
	y2ACDelta   int
	uvDCDelta   int
	uvACDelta   int
	skipEnabled bool
	skipProb    uint8
	filterLevel int
	sharpness   int
	// Inter-frame fields.
	refreshLast   bool
	refreshGolden bool
	refreshAltref bool
	probIntra     uint8
	probLast      uint8
	probGolden    uint8
	mvProbs       [2][19]uint8
}

//nolint:funlen // VP8 header parsing has many sequential fields per RFC 6386.
func (d *Decoder) parseHeader(br *boolReader, keyframe bool) (frameHeader, error) {
	var fh frameHeader

	if keyframe {
		_ = br.readBool(128) // color_space
		_ = br.readBool(128) // clamping
	}

	// Segmentation.
	if br.readBool(128) {
		// Skip segmentation parsing for now - just consume bits.
		updateMap := br.readBool(128)
		if br.readBool(128) {
			_ = br.readBool(128) // abs vs delta
			for i := 0; i < 4; i++ {
				if br.readBool(128) {
					_ = br.readSignedLit(7)
				}
			}
			for i := 0; i < 4; i++ {
				if br.readBool(128) {
					_ = br.readSignedLit(6)
				}
			}
		}
		if updateMap {
			for i := 0; i < 3; i++ {
				if br.readBool(128) {
					_ = br.readLit(8)
				}
			}
		}
	}

	// Filter.
	_ = br.readBool(128) // filter_type
	fh.filterLevel = int(br.readLit(6))
	fh.sharpness = int(br.readLit(3))

	// Loop filter adjustments.
	if br.readBool(128) {
		_ = br.readBool(128) // mode ref delta enabled
		if br.readBool(128) {
			for i := 0; i < 4; i++ {
				if br.readBool(128) {
					_ = br.readSignedLit(6)
				}
			}
			for i := 0; i < 4; i++ {
				if br.readBool(128) {
					_ = br.readSignedLit(6)
				}
			}
		}
	}

	// Token partitions.
	nParts := 1 << br.readLit(2)
	if nParts != 1 {
		return fh, fmt.Errorf("vp8: %d token partitions not supported", nParts)
	}

	// Quantizer.
	fh.yACQI = int(br.readLit(7))
	if br.readBool(128) {
		fh.yDCDelta = int(br.readSignedLit(4))
	}
	if br.readBool(128) {
		fh.y2DCDelta = int(br.readSignedLit(4))
	}
	if br.readBool(128) {
		fh.y2ACDelta = int(br.readSignedLit(4))
	}
	if br.readBool(128) {
		fh.uvDCDelta = int(br.readSignedLit(4))
	}
	if br.readBool(128) {
		fh.uvACDelta = int(br.readSignedLit(4))
	}

	if !keyframe {
		fh.refreshGolden = br.readBool(128)
		fh.refreshAltref = br.readBool(128)
		if !fh.refreshGolden {
			_ = br.readLit(2) // golden copy flag
		}
		if !fh.refreshAltref {
			_ = br.readLit(2) // altref copy flag
		}
		_ = br.readBool(128) // sign bias golden
		_ = br.readBool(128) // sign bias altref
		fh.refreshLast = true
	}

	// Refresh entropy probs.
	_ = br.readBool(128)

	if !keyframe {
		// Refresh last.
		fh.refreshLast = br.readBool(128)
	}

	// Coeff prob updates.
	fh.coeffProbs = DefaultCoeffProbs
	for i := range fh.coeffProbs {
		for j := range fh.coeffProbs[i] {
			for k := range fh.coeffProbs[i][j] {
				for l := range fh.coeffProbs[i][j][k] {
					if br.readBool(CoeffUpdateProbs[i][j][k][l]) {
						fh.coeffProbs[i][j][k][l] = uint8(br.readLit(8))
					}
				}
			}
		}
	}

	// Skip.
	fh.skipEnabled = br.readBool(128)
	if fh.skipEnabled {
		fh.skipProb = uint8(br.readLit(8))
	}

	if !keyframe {
		// Inter-frame prob updates.
		fh.probIntra = uint8(br.readLit(8))
		fh.probLast = uint8(br.readLit(8))
		fh.probGolden = uint8(br.readLit(8))
		// Intra 16x16 mode probs update.
		if br.readBool(128) {
			for i := 0; i < 4; i++ {
				_ = br.readLit(8)
			}
		}
		// Intra chroma mode probs update.
		if br.readBool(128) {
			for i := 0; i < 3; i++ {
				_ = br.readLit(8)
			}
		}
		// MV probs update.
		fh.mvProbs = defaultMVProbs
		for i := 0; i < 2; i++ {
			for j := 0; j < 19; j++ {
				if br.readBool(mvUpdateProbs[i][j]) {
					fh.mvProbs[i][j] = uint8(br.readLit(7)) << 1
					if fh.mvProbs[i][j] == 0 {
						fh.mvProbs[i][j] = 1
					}
				}
			}
		}
	}

	return fh, nil
}

//nolint:funlen // VP8 MB decoding requires many steps per RFC 6386.
func (d *Decoder) decodeMB(
	br, tbr *boolReader, frame *Frame, mbX, mbY int, fh *frameHeader, keyframe bool,
) error {
	yStride := frame.yStride
	uvStride := frame.uvStride

	// Quantizers.
	yDCQ := int(dcQLookup[clampQP(fh.yACQI+fh.yDCDelta)])
	yACQ := int(acQLookup[clampQP(fh.yACQI)])
	y2DCQ := int(dcQLookup[clampQP(fh.yACQI+fh.y2DCDelta)]) * 2
	y2ACQ := int(acQLookup[clampQP(fh.yACQI+fh.y2ACDelta)]) * 155 / 100
	if y2ACQ < 8 {
		y2ACQ = 8
	}
	uvDCQ := int(dcQLookup[clampQP(fh.yACQI+fh.uvDCDelta)])
	uvACQ := int(acQLookup[clampQP(fh.yACQI+fh.uvACDelta)])

	yOff := mbY*16*yStride + mbX*16
	uOff := mbY*8*uvStride + mbX*8
	vOff := uOff

	var isInter bool
	var yMode, uvMode int
	var mbMV mv
	var bModes [16]int
	skip := false

	if keyframe {
		if fh.skipEnabled {
			skip = br.readBool(fh.skipProb)
		}
		yMode = readKFYMode(br)
		if yMode == predBPRD {
			bModes = readKFBPredModes(br, d.bModeAboveRow[mbX], d.bModeLeft)
			// Update context: bottom row (12,13,14,15) and right column (3,7,11,15).
			d.bModeAboveRow[mbX] = [4]int{bModes[12], bModes[13], bModes[14], bModes[15]}
			d.bModeLeft = [4]int{bModes[3], bModes[7], bModes[11], bModes[15]}
		} else {
			bm := yModeToBMode(yMode)
			d.bModeAboveRow[mbX] = [4]int{bm, bm, bm, bm}
			d.bModeLeft = [4]int{bm, bm, bm, bm}
		}
		uvMode = readKFUVMode(br)
	} else {
		// Inter-frame MB mode.
		isInter = !br.readBool(fh.probIntra)
		if isInter {
			// Reference frame selection.
			refLast := !br.readBool(fh.probLast)
			if !refLast {
				_ = br.readBool(fh.probGolden) // golden vs altref
			}
			// MV mode: ZEROMV, NEARESTMV, NEARMV, NEWMV, SPLITMV.
			// Simplified: read MV for the whole MB.
			mvMode := readInterMBMode(br)
			switch mvMode {
			case 0: // NEARESTMV
				mbMV = d.findNearestMV(frame, mbX, mbY)
			case 1: // NEARMV
				mbMV = d.findNearMV(frame, mbX, mbY)
			case 2: // ZEROMV
				mbMV = mv{}
			case 3: // NEWMV
				delta := readMV(br, &fh.mvProbs)
				nearest := d.findNearestMV(frame, mbX, mbY)
				mbMV = mv{x: nearest.x + delta.x, y: nearest.y + delta.y}
			}
		} else {
			yMode = readInterIntraYMode(br)
			if yMode == predBPRD {
				bModes = readInterBPredModes(br)
			}
			uvMode = readInterIntraUVMode(br)
		}
	}

	// Skip.
	if !keyframe && fh.skipEnabled {
		skip = br.readBool(fh.skipProb)
	}

	// Prediction.
	if isInter {
		ref := d.last
		if mbMV.x == 0 && mbMV.y == 0 {
			// ZEROMV fast path: direct copy from reference.
			for row := 0; row < 16; row++ {
				srcOff := (mbY*16+row)*ref.yStride + mbX*16
				dstOff := (mbY*16+row)*yStride + mbX*16
				copy(frame.Y[dstOff:dstOff+16], ref.Y[srcOff:srcOff+16])
			}
			for row := 0; row < 8; row++ {
				srcOff := (mbY*8+row)*ref.uvStride + mbX*8
				dstOff := (mbY*8+row)*uvStride + mbX*8
				copy(frame.U[dstOff:dstOff+8], ref.U[srcOff:srcOff+8])
				copy(frame.V[dstOff:dstOff+8], ref.V[srcOff:srcOff+8])
			}
		} else {
			interPredBlock(frame.Y, yStride, ref.Y, ref.yStride,
				ref.yStride, len(ref.Y)/ref.yStride, 16, 16, mbMV.x, mbMV.y, mbX*16, mbY*16)
			cMV := mv{x: mbMV.x / 2, y: mbMV.y / 2}
			interPredBlock(frame.U, uvStride, ref.U, ref.uvStride,
				ref.uvStride, len(ref.U)/ref.uvStride, 8, 8, cMV.x, cMV.y, mbX*8, mbY*8)
			interPredBlock(frame.V, uvStride, ref.V, ref.uvStride,
				ref.uvStride, len(ref.V)/ref.uvStride, 8, 8, cMV.x, cMV.y, mbX*8, mbY*8)
		}
	} else if yMode == predBPRD {
		// B_PRED: predict and add residual per sub-block.
		// UV prediction first.
		var uAbove, vAbove []byte
		if mbY > 0 {
			uAbove = frame.U[(mbY*8-1)*uvStride+mbX*8 : (mbY*8-1)*uvStride+mbX*8+8]
			vAbove = frame.V[(mbY*8-1)*uvStride+mbX*8 : (mbY*8-1)*uvStride+mbX*8+8]
		}
		var uLeft, vLeft []byte
		if mbX > 0 {
			uLeft = make([]byte, 8)
			vLeft = make([]byte, 8)
			for i := 0; i < 8; i++ {
				uLeft[i] = frame.U[(mbY*8+i)*uvStride+mbX*8-1]
				vLeft[i] = frame.V[(mbY*8+i)*uvStride+mbX*8-1]
			}
		}
		var uvAL byte = 128
		if mbX > 0 && mbY > 0 {
			uvAL = frame.U[(mbY*8-1)*uvStride+mbX*8-1]
		}
		predictUV8(frame.U[uOff:], uvStride, uvMode, uAbove, uLeft, uvAL)
		var vAL byte = 128
		if mbX > 0 && mbY > 0 {
			vAL = frame.V[(mbY*8-1)*uvStride+mbX*8-1]
		}
		predictUV8(frame.V[vOff:], uvStride, uvMode, vAbove, vLeft, vAL)

		if !skip {
			// B_PRED: decode each Y sub-block with full DCT (DC+AC), no Y2.
			for by := 0; by < 4; by++ {
				for bx := 0; bx < 4; bx++ {
					i := by*4 + bx
					bOff := yOff + by*4*yStride + bx*4

					// Get context for this sub-block from already-reconstructed frame.
					var above [8]byte
					var left [4]byte
					var al byte
					getDecoderSubBlockContext(frame.Y, yStride, mbX, mbY, bx, by, above[:], left[:], &al)

					// Predict.
					predict4x4(frame.Y[bOff:], yStride, bModes[i], above[:], left[:], al)

					// Decode and add residual.
					var coeffs [16]int16
					initCtx := boolInt(d.topNNZY[mbX*4+bx]) + boolInt(d.leftNNZY[by])
					nz := decodeBPredBlock(tbr, coeffs[:], &fh.coeffProbs, initCtx)
					d.topNNZY[mbX*4+bx] = nz
					d.leftNNZY[by] = nz
					coeffs[0] *= int16(yDCQ)
					for j := 1; j < 16; j++ {
						coeffs[j] *= int16(yACQ)
					}
					residual := idct4x4(coeffs)
					for row := 0; row < 4; row++ {
						for col := 0; col < 4; col++ {
							off := bOff + row*yStride + col
							frame.Y[off] = clampByte(int(frame.Y[off]) + int(residual[row*4+col]))
						}
					}
				}
			}

			// Y2 unused for B_PRED.
			d.topNNZY2[mbX] = false
			d.leftY2 = false

			// U/V residual.
			for by := 0; by < 2; by++ {
				for bx := 0; bx < 2; bx++ {
					var coeffs [16]int16
					initCtx := boolInt(d.topNNZU[mbX*2+bx]) + boolInt(d.leftNNZU[by])
					nz := decodeBlock(tbr, coeffs[:], 2, &fh.coeffProbs, initCtx)
					d.topNNZU[mbX*2+bx] = nz
					d.leftNNZU[by] = nz
					coeffs[0] *= int16(uvDCQ)
					for i := 1; i < 16; i++ {
						coeffs[i] *= int16(uvACQ)
					}
					residual := idct4x4(coeffs)
					for row := 0; row < 4; row++ {
						for col := 0; col < 4; col++ {
							off := uOff + (by*4+row)*uvStride + bx*4 + col
							frame.U[off] = clampByte(int(frame.U[off]) + int(residual[row*4+col]))
						}
					}
				}
			}
			for by := 0; by < 2; by++ {
				for bx := 0; bx < 2; bx++ {
					var coeffs [16]int16
					initCtx := boolInt(d.topNNZV[mbX*2+bx]) + boolInt(d.leftNNZV[by])
					nz := decodeBlock(tbr, coeffs[:], 2, &fh.coeffProbs, initCtx)
					d.topNNZV[mbX*2+bx] = nz
					d.leftNNZV[by] = nz
					coeffs[0] *= int16(uvDCQ)
					for i := 1; i < 16; i++ {
						coeffs[i] *= int16(uvACQ)
					}
					residual := idct4x4(coeffs)
					for row := 0; row < 4; row++ {
						for col := 0; col < 4; col++ {
							off := vOff + (by*4+row)*uvStride + bx*4 + col
							frame.V[off] = clampByte(int(frame.V[off]) + int(residual[row*4+col]))
						}
					}
				}
			}
		} else {
			// Skip: just predict each sub-block, no residual.
			for i := 0; i < 16; i++ {
				by := i / 4
				bx := i % 4
				bOff := yOff + by*4*yStride + bx*4
				var above [8]byte
				var left [4]byte
				var al byte
				getDecoderSubBlockContext(frame.Y, yStride, mbX, mbY, bx, by, above[:], left[:], &al)
				predict4x4(frame.Y[bOff:], yStride, bModes[i], above[:], left[:], al)
			}
		}
		return nil
	} else {
		// Intra prediction (non-B_PRED).
		var yAbove []byte
		if mbY > 0 {
			yAbove = frame.Y[(mbY*16-1)*yStride+mbX*16 : (mbY*16-1)*yStride+mbX*16+16]
		}
		var yLeft []byte
		if mbX > 0 {
			yLeft = make([]byte, 16)
			for i := 0; i < 16; i++ {
				yLeft[i] = frame.Y[(mbY*16+i)*yStride+mbX*16-1]
			}
		}
		var yAL byte = 128
		if mbX > 0 && mbY > 0 {
			yAL = frame.Y[(mbY*16-1)*yStride+mbX*16-1]
		}
		predictY16(frame.Y[yOff:], yStride, yMode, yAbove, yLeft, yAL)

		var uAbove, vAbove []byte
		if mbY > 0 {
			uAbove = frame.U[(mbY*8-1)*uvStride+mbX*8 : (mbY*8-1)*uvStride+mbX*8+8]
			vAbove = frame.V[(mbY*8-1)*uvStride+mbX*8 : (mbY*8-1)*uvStride+mbX*8+8]
		}
		var uLeft, vLeft []byte
		if mbX > 0 {
			uLeft = make([]byte, 8)
			vLeft = make([]byte, 8)
			for i := 0; i < 8; i++ {
				uLeft[i] = frame.U[(mbY*8+i)*uvStride+mbX*8-1]
				vLeft[i] = frame.V[(mbY*8+i)*uvStride+mbX*8-1]
			}
		}
		var uvAL byte = 128
		if mbX > 0 && mbY > 0 {
			uvAL = frame.U[(mbY*8-1)*uvStride+mbX*8-1]
		}
		predictUV8(frame.U[uOff:], uvStride, uvMode, uAbove, uLeft, uvAL)
		predictUV8(frame.V[vOff:], uvStride, uvMode, vAbove, vLeft, uvAL)
	}

	if skip {
		return nil
	}

	// Decode residual.
	var y2Coeffs [16]int16
	y2Ctx := boolInt(d.topNNZY2[mbX]) + boolInt(d.leftY2)
	y2nz := decodeBlock(tbr, y2Coeffs[:], 1, &fh.coeffProbs, y2Ctx)

	d.topNNZY2[mbX] = y2nz
	d.leftY2 = y2nz
	y2Coeffs[0] *= int16(y2DCQ)
	for i := 1; i < 16; i++ {
		y2Coeffs[i] *= int16(y2ACQ)
	}
	yDCs := iwht4x4(y2Coeffs)

	for by := 0; by < 4; by++ {
		for bx := 0; bx < 4; bx++ {
			var coeffs [16]int16
			ctx := boolInt(d.topNNZY[mbX*4+bx]) + boolInt(d.leftNNZY[by])
			nz := decodeBlock(tbr, coeffs[:], 0, &fh.coeffProbs, ctx)
			d.topNNZY[mbX*4+bx] = nz
			d.leftNNZY[by] = nz
			for i := 1; i < 16; i++ {
				coeffs[i] *= int16(yACQ)
			}
			coeffs[0] = yDCs[by*4+bx]
			residual := idct4x4(coeffs)
			for row := 0; row < 4; row++ {
				for col := 0; col < 4; col++ {
					off := yOff + (by*4+row)*yStride + bx*4 + col
					frame.Y[off] = clampByte(int(frame.Y[off]) + int(residual[row*4+col]))
				}
			}
		}
	}

	for by := 0; by < 2; by++ {
		for bx := 0; bx < 2; bx++ {
			var coeffs [16]int16
			ctx := boolInt(d.topNNZU[mbX*2+bx]) + boolInt(d.leftNNZU[by])
			nz := decodeBlock(tbr, coeffs[:], 2, &fh.coeffProbs, ctx)
			d.topNNZU[mbX*2+bx] = nz
			d.leftNNZU[by] = nz
			coeffs[0] *= int16(uvDCQ)
			for i := 1; i < 16; i++ {
				coeffs[i] *= int16(uvACQ)
			}
			residual := idct4x4(coeffs)
			for row := 0; row < 4; row++ {
				for col := 0; col < 4; col++ {
					off := uOff + (by*4+row)*uvStride + bx*4 + col
					frame.U[off] = clampByte(int(frame.U[off]) + int(residual[row*4+col]))
				}
			}
		}
	}

	for by := 0; by < 2; by++ {
		for bx := 0; bx < 2; bx++ {
			var coeffs [16]int16
			ctx := boolInt(d.topNNZV[mbX*2+bx]) + boolInt(d.leftNNZV[by])
			nz := decodeBlock(tbr, coeffs[:], 2, &fh.coeffProbs, ctx)
			d.topNNZV[mbX*2+bx] = nz
			d.leftNNZV[by] = nz
			coeffs[0] *= int16(uvDCQ)
			for i := 1; i < 16; i++ {
				coeffs[i] *= int16(uvACQ)
			}
			residual := idct4x4(coeffs)
			for row := 0; row < 4; row++ {
				for col := 0; col < 4; col++ {
					off := vOff + (by*4+row)*uvStride + bx*4 + col
					frame.V[off] = clampByte(int(frame.V[off]) + int(residual[row*4+col]))
				}
			}
		}
	}

	return nil
}

func readInterMBMode(br *boolReader) int {
	// Simplified inter MB mode tree.
	// 0=NEARESTMV, 1=NEARMV, 2=ZEROMV, 3=NEWMV
	if !br.readBool(162) {
		return 2 // ZEROMV (most common)
	}
	if !br.readBool(128) {
		return 0 // NEARESTMV
	}
	if !br.readBool(128) {
		return 1 // NEARMV
	}
	return 3 // NEWMV
}

func readInterIntraYMode(br *boolReader) int {
	// Inter-frame ymode_tree (RFC 6386 section 16.1):
	// prob[0]: DC vs rest; prob[1]: {V,H} vs {TM,B_PRED}
	// prob[2]: V vs H; prob[3]: TM vs B_PRED
	if !br.readBool(interYModeProbs[0]) {
		return predDC
	}
	if !br.readBool(interYModeProbs[1]) {
		if !br.readBool(interYModeProbs[2]) {
			return predV
		}
		return predH
	}
	if !br.readBool(interYModeProbs[3]) {
		return predTM
	}
	return predBPRD
}

func readInterIntraUVMode(br *boolReader) int {
	return readKFUVMode(br)
}

// readInterBPredModes reads 16 sub-block modes for B_PRED in inter-frames.
// Uses fixed probability table (RFC 6386 section 16.1).
func readInterBPredModes(br *boolReader) [16]int {
	var modes [16]int
	for i := 0; i < 16; i++ {
		modes[i] = readKFSubBlockMode(br, interBModeProbs[:])
	}
	return modes
}

// findNearestMV returns the nearest MV from neighboring MBs.
func (d *Decoder) findNearestMV(_ *Frame, _, _ int) mv {
	// TODO: proper MV prediction from neighbors.
	return mv{}
}

// findNearMV returns the near MV from neighboring MBs.
func (d *Decoder) findNearMV(_ *Frame, _, _ int) mv {
	return mv{}
}

// readKFYMode reads the keyframe Y prediction mode.
func readKFYMode(br *boolReader) int {
	if !br.readBool(defaultYModeProbs[0]) {
		return predBPRD
	}
	if !br.readBool(defaultYModeProbs[1]) {
		if !br.readBool(defaultYModeProbs[2]) {
			return predDC
		}
		return predV
	}
	if !br.readBool(defaultYModeProbs[3]) {
		return predH
	}
	return predTM
}

// readKFUVMode reads the keyframe UV prediction mode.
func readKFUVMode(br *boolReader) int {
	if !br.readBool(defaultUVModeProbs[0]) {
		return predDC
	}
	if !br.readBool(defaultUVModeProbs[1]) {
		return predV
	}
	if !br.readBool(defaultUVModeProbs[2]) {
		return predH
	}
	return predTM
}

// decodeBlock decodes a 4x4 block of quantized coefficients.
// Returns true if the block has any non-zero coefficient.
func decodeBlock(br *boolReader, coeffs []int16, plane int, probs *[4][8][3][11]uint8, initCtx int) bool {
	startIdx := 0
	if plane == 0 {
		startIdx = 1
	}
	ctx := initCtx
	i := startIdx
	band := coeffBand(i)
	p := probs[plane][band][ctx]
	// Wrapper EOB check (first coefficient).
	if !br.readBool(p[0]) {
		return false
	}
	hasNZ := false
	// Enter at skip_eob for first coefficient.
	for {
		p = probs[plane][coeffBand(i)][ctx]
		if !br.readBool(p[1]) {
			// Zero.
			ctx = 0
			i++
			if i >= 16 {
				return hasNZ
			}
			// After zero: skip EOB, go directly to next zero/nz check.
			continue
		}
		// Non-zero.
		hasNZ = true
		absV := decodeTokenValue(br, p)
		if br.readBool(128) {
			coeffs[zigzag[i]] = -absV
		} else {
			coeffs[zigzag[i]] = absV
		}
		if absV == 1 {
			ctx = 1
		} else {
			ctx = 2
		}
		i++
		if i >= 16 {
			return hasNZ
		}
		// After non-zero: check EOB for next coefficient.
		p = probs[plane][coeffBand(i)][ctx]
		if !br.readBool(p[0]) {
			return hasNZ
		}
	}
}

// decodeTokenValue decodes the magnitude of a non-zero coefficient.
func decodeTokenValue(br *boolReader, probs [11]uint8) int16 {
	if !br.readBool(probs[2]) {
		return 1
	}
	if !br.readBool(probs[3]) {
		// 2, 3, or 4.
		if !br.readBool(probs[4]) {
			return 2
		}
		if !br.readBool(probs[5]) {
			return 3
		}
		return 4
	}
	if !br.readBool(probs[6]) {
		if !br.readBool(probs[7]) {
			return 5 + int16(readExtra(br, tokenExtraBits[5].prob))
		}
		return 7 + int16(readExtra(br, tokenExtraBits[6].prob))
	}
	if !br.readBool(probs[8]) {
		if !br.readBool(probs[9]) {
			return 11 + int16(readExtra(br, tokenExtraBits[7].prob))
		}
		return 19 + int16(readExtra(br, tokenExtraBits[8].prob))
	}
	if !br.readBool(probs[10]) {
		return 35 + int16(readExtra(br, tokenExtraBits[9].prob))
	}
	return 67 + int16(readExtra(br, tokenExtraBits[10].prob))
}

func readExtra(br *boolReader, probs []uint8) int {
	val := 0
	for _, p := range probs {
		val <<= 1
		if br.readBool(p) {
			val |= 1
		}
	}
	return val
}

func clampByte(v int) byte {
	if v < 0 {
		return 0
	}
	if v > 255 {
		return 255
	}
	return byte(v)
}

// MV probability update table from RFC 6386 section 17.1.
//
//nolint:gochecknoglobals // RFC 6386 spec data.
var mvUpdateProbs = [2][19]uint8{
	{237, 246, 253, 253, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 250, 250, 252, 254, 254},
	{231, 243, 245, 253, 254, 254, 254, 254, 254, 254, 254, 254, 254, 254, 251, 251, 254, 254, 254},
}

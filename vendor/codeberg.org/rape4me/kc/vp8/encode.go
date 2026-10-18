package vp8

import (
	"encoding/binary"
)

// Encoder produces valid VP8 frames (keyframes and inter-frames).
type Encoder struct {
	width, height int
	qp            int // quantizer parameter 0-127
	frameCount    int
	keyInterval   int // keyframe every N frames (0 = all keyframes)
	forceKey      bool
	lastY         []byte
	lastU, lastV  []byte
	padW, padH    int

	// Pre-allocated buffers to avoid per-frame allocations.
	padBuf     []byte
	reconBuf   []byte // reconstructed Y plane for encoder-decoder consistency
	uvBuf      []byte
	coeffParts *boolWriter
	fpParts    *boolWriter
	mbInfoBuf  []mbInfo
}

// NewEncoder creates an encoder for the given frame dimensions.
// qp is the quantizer parameter (0=best quality, 127=smallest).
func NewEncoder(width, height, qp int) *Encoder {
	mbW := (width + 15) >> 4
	mbH := (height + 15) >> 4
	padW := mbW * 16
	padH := mbH * 16
	estSize := padW * padH / 4
	return &Encoder{
		width:       width,
		height:      height,
		qp:          qp,
		keyInterval: 30,
		padBuf:      make([]byte, padW*padH),
		reconBuf:    make([]byte, padW*padH),
		uvBuf:       make([]byte, (padW/2)*(padH/2)),
		coeffParts:  newBoolWriterCap(estSize),
		fpParts:     newBoolWriterCap(estSize / 4),
	}
}

// SetKeyInterval sets how often to emit keyframes. 0 = every frame is a keyframe.
func (e *Encoder) SetKeyInterval(n int) {
	e.keyInterval = n
}

// ForceNextKeyframe makes the next Encode call produce a keyframe regardless of keyInterval.
func (e *Encoder) ForceNextKeyframe() {
	e.forceKey = true
}

// Encode encodes a grayscale frame. Automatically chooses keyframe or inter-frame
// based on keyInterval. Returns the VP8 bitstream.
func (e *Encoder) Encode(gray []byte) ([]byte, error) {
	isKey := e.keyInterval == 0 || e.frameCount%e.keyInterval == 0 || e.lastY == nil || e.forceKey
	e.forceKey = false
	e.frameCount++

	if isKey {
		return e.encodeKeyframe(gray)
	}
	return e.encodeInterFrame(gray)
}

// mbInfo holds per-macroblock encoding decisions.
type mbInfo struct {
	yMode  int
	bModes [16]int // sub-block modes (only for B_PRED)
	skip   bool    // true if all coefficients are zero
}

// encodeKeyframe produces a VP8 keyframe (I-frame).
//
//nolint:funlen,gocognit,gocyclo,maintidx // VP8 keyframe encoding per RFC 6386.
func (e *Encoder) encodeKeyframe(gray []byte) ([]byte, error) {
	w, h := e.width, e.height
	mbW := (w + 15) >> 4
	mbH := (h + 15) >> 4

	// Pad frame to macroblock boundary.
	padW := mbW * 16
	padH := mbH * 16
	y := padFrameInto(e.padBuf, gray, w, h, padW, padH)

	// Quantizer indices.
	qp := clampQP(e.qp)
	yDCQ := int(dcQLookup[qp])
	yACQ := int(acQLookup[qp])
	y2DCQ := int(dcQLookup[qp]) * 2
	y2ACQ := int(acQLookup[qp]) * 155 / 100
	if y2ACQ < 8 {
		y2ACQ = 8
	}

	// Single pass: decide mode, encode coefficients, and reconstruct each MB.
	// We maintain reconBuf so subsequent MBs use correct decoder-matching context.
	recon := e.reconBuf[:padW*padH]
	// Pre-copy source to recon. For uniform MBs where DC prediction matches source,
	// the recon is already correct (no writes needed in chooseBPredModes).
	copy(recon, y)
	if len(e.mbInfoBuf) < mbW*mbH {
		e.mbInfoBuf = make([]mbInfo, mbW*mbH)
	}
	mbInfos := e.mbInfoBuf[:mbW*mbH]
	e.coeffParts.reset()
	coeffParts := e.coeffParts

	// Non-zero tracking for token coefficient context. VP8 spec: initial ctx
	// for decoding a block's coefficients = (top_neighbor_has_nonzero ? 1 : 0)
	// + (left_neighbor_has_nonzero ? 1 : 0). Tracked per sub-block column for
	// top and per row within MB for left.
	topNNZY := make([]bool, 4*mbW)
	topNNZU := make([]bool, 2*mbW)
	topNNZV := make([]bool, 2*mbW)
	topNNZY2 := make([]bool, mbW)

	for mbY := 0; mbY < mbH; mbY++ {
		var leftNNZY [4]bool
		var leftNNZU [2]bool
		var leftNNZV [2]bool
		var leftNNZY2 bool
		for mbX := 0; mbX < mbW; mbX++ {
			info := &mbInfos[mbY*mbW+mbX]

			// Compute B_PRED using reconstruction buffer for border context.
			modes, bpQCoeffs, mbAllZero := chooseBPredModes(y, recon, padW, mbX, mbY, yDCQ, yACQ)
			// TODO: DC_PRED produces frames that ffmpeg/libvpx decode incorrectly
			// for some patterns. Root cause unknown despite extensive analysis.
			// Forcing BPRED gives bit-correct output for all tested patterns.
			if true {
				info.yMode = predBPRD
				info.bModes = modes

				if mbAllZero {
					info.skip = true
				} else {
					info.skip = false
					// Encode B_PRED sub-blocks with NNZ-based context.
					for by := 0; by < 4; by++ {
						for bx := 0; bx < 4; bx++ {
							i := by*4 + bx
							top := topNNZY[mbX*4+bx]
							left := leftNNZY[by]
							initCtx := boolInt(top) + boolInt(left)
							encodeBPredBlock(coeffParts, bpQCoeffs[i][:], initCtx)
							nz := hasNonZero(bpQCoeffs[i][:])
							topNNZY[mbX*4+bx] = nz
							leftNNZY[by] = nz
						}
					}
				}

				// Y2 unused for B_PRED - clear NNZ.
				topNNZY2[mbX] = false
				leftNNZY2 = false
			} else {
				info.yMode = predDC

				// Compute actual DC_PRED prediction (neighbor-based, matching decoder).
				// Also build the 16x16 prediction block for residual computation.
				var pred16 [256]byte
				var dcVal int
				if mbX > 0 && mbY > 0 {
					// Use both top and left.
					sum := 0
					for i := 0; i < 16; i++ {
						sum += int(recon[(mbY*16-1)*padW+mbX*16+i])
						sum += int(recon[(mbY*16+i)*padW+mbX*16-1])
					}
					dcVal = (sum + 16) >> 5
				} else if mbY > 0 {
					sum := 0
					for i := 0; i < 16; i++ {
						sum += int(recon[(mbY*16-1)*padW+mbX*16+i])
					}
					dcVal = (sum + 8) >> 4
				} else if mbX > 0 {
					sum := 0
					for i := 0; i < 16; i++ {
						sum += int(recon[(mbY*16+i)*padW+mbX*16-1])
					}
					dcVal = (sum + 8) >> 4
				} else {
					dcVal = 128
				}
				for i := 0; i < 256; i++ {
					pred16[i] = byte(dcVal)
				}

				// DC_PRED: Y2 + AC sub-blocks.
				var yDCs [16]int16
				var yACCoeffs [16][16]int16
				for by := 0; by < 4; by++ {
					for bx := 0; bx < 4; bx++ {
						var block [16]int16
						for row := 0; row < 4; row++ {
							py := mbY*16 + by*4 + row
							for col := 0; col < 4; col++ {
								px := mbX*16 + bx*4 + col
								predIdx := (by*4+row)*16 + bx*4 + col
								block[row*4+col] = int16(y[py*padW+px]) - int16(pred16[predIdx])
							}
						}
						dct := fdct4x4(block)
						yDCs[by*4+bx] = dct[0]
						dct[0] = 0
						idx := by*4 + bx
						for i := 1; i < 16; i++ {
							yACCoeffs[idx][i] = quantize(dct[i], yACQ)
						}
					}
				}
				wht := fwht4x4(yDCs)
				var y2q [16]int16
				y2q[0] = quantize(wht[0], y2DCQ)
				for i := 1; i < 16; i++ {
					y2q[i] = quantize(wht[i], y2ACQ)
				}
				// Y2 block.
				y2Ctx := boolInt(topNNZY2[mbX]) + boolInt(leftNNZY2)
				encodeBlock(coeffParts, y2q[:], 1, y2Ctx)
				y2nz := hasNonZero(y2q[:])
				topNNZY2[mbX] = y2nz
				leftNNZY2 = y2nz

				// 16 Y AC sub-blocks.
				for by := 0; by < 4; by++ {
					for bx := 0; bx < 4; bx++ {
						i := by*4 + bx
						top := topNNZY[mbX*4+bx]
						left := leftNNZY[by]
						initCtx := boolInt(top) + boolInt(left)
						encodeBlock(coeffParts, yACCoeffs[i][:], 0, initCtx)
						nz := hasNonZero(yACCoeffs[i][:])
						topNNZY[mbX*4+bx] = nz
						leftNNZY[by] = nz
					}
				}

				// Reconstruct DC_PRED MB into recon buffer.
				// Dequantize Y2.
				var y2dq [16]int16
				y2dq[0] = y2q[0] * int16(y2DCQ)
				for i := 1; i < 16; i++ {
					y2dq[i] = y2q[i] * int16(y2ACQ)
				}
				reconDCs := iwht4x4(y2dq)
				for by := 0; by < 4; by++ {
					for bx := 0; bx < 4; bx++ {
						idx := by*4 + bx
						var dqcoeffs [16]int16
						dqcoeffs[0] = reconDCs[idx]
						for i := 1; i < 16; i++ {
							dqcoeffs[i] = yACCoeffs[idx][i] * int16(yACQ)
						}
						residual := idct4x4(dqcoeffs)
						for row := 0; row < 4; row++ {
							for col := 0; col < 4; col++ {
								off := (mbY*16+by*4+row)*padW + mbX*16 + bx*4 + col
								predIdx := (by*4+row)*16 + bx*4 + col
								v := int(pred16[predIdx]) + int(residual[row*4+col])
								if v < 0 {
									v = 0
								} else if v > 255 {
									v = 255
								}
								recon[off] = byte(v)
							}
						}
					}
				}
			}

			// U/V blocks - always zero for grayscale.
			if !info.skip {
				// Write EOB directly for non-skipped MBs.
				for by := 0; by < 2; by++ {
					for bx := 0; bx < 2; bx++ {
						initCtx := boolInt(topNNZU[mbX*2+bx]) + boolInt(leftNNZU[by])
						p := DefaultCoeffProbs[2][coeffBand(0)][initCtx]
						coeffParts.writeBool(p[0], false)
						topNNZU[mbX*2+bx] = false
						leftNNZU[by] = false
					}
				}
				for by := 0; by < 2; by++ {
					for bx := 0; bx < 2; bx++ {
						initCtx := boolInt(topNNZV[mbX*2+bx]) + boolInt(leftNNZV[by])
						p := DefaultCoeffProbs[2][coeffBand(0)][initCtx]
						coeffParts.writeBool(p[0], false)
						topNNZV[mbX*2+bx] = false
						leftNNZV[by] = false
					}
				}
			}
			// For skipped MBs, decoder doesn't read UV coefficients and
			// doesn't modify NNZ state, so we leave it unchanged.
		}
	}
	coeffParts.flush()

	// Store reconstructed frame as reference (must match decoder output).
	e.padW = padW
	e.padH = padH
	if cap(e.lastY) < len(recon) {
		e.lastY = make([]byte, len(recon))
	}
	e.lastY = e.lastY[:len(recon)]
	copy(e.lastY, recon)
	uvSize := (padW / 2) * (padH / 2)
	if len(e.lastU) != uvSize {
		e.lastU = make([]byte, uvSize)
		e.lastV = make([]byte, uvSize)
		for i := range e.lastU {
			e.lastU[i] = 128
			e.lastV[i] = 128
		}
	}

	// Build the frame.
	return e.buildKeyframe(mbW, mbH, qp, mbInfos, coeffParts.bytes()), nil
}

// encodeInterFrame produces a VP8 inter-frame (P-frame) using motion estimation.
//
//nolint:funlen // VP8 inter-frame encoding per RFC 6386.
func (e *Encoder) encodeInterFrame(gray []byte) ([]byte, error) {
	w, h := e.width, e.height
	mbW := (w + 15) >> 4
	mbH := (h + 15) >> 4
	padW := mbW * 16
	padH := mbH * 16

	curY := padFrameInto(e.padBuf, gray, w, h, padW, padH)

	qp := clampQP(e.qp)
	yACQ := int(acQLookup[qp])

	// First partition: header + MB modes.
	e.fpParts.reset()
	fp := e.fpParts

	// Segmentation disabled.
	fp.writeBool(128, false)
	// Filter.
	fp.writeBool(128, false) // normal
	fp.writeLit(6, 0)        // filter level
	fp.writeLit(3, 0)        // sharpness
	// Loop filter adj disabled.
	fp.writeBool(128, false)
	// Token partitions: 1.
	fp.writeLit(2, 0)
	// Quantizer.
	fp.writeLit(7, uint32(qp))
	fp.writeBool(128, false) // y_dc_delta
	fp.writeBool(128, false) // y2_dc_delta
	fp.writeBool(128, false) // y2_ac_delta
	fp.writeBool(128, false) // uv_dc_delta
	fp.writeBool(128, false) // uv_ac_delta

	// Refresh golden/altref: no.
	fp.writeBool(128, false) // refresh_golden
	fp.writeBool(128, false) // refresh_altref
	fp.writeLit(2, 0)        // golden copy
	fp.writeLit(2, 0)        // altref copy
	fp.writeBool(128, false) // sign bias golden
	fp.writeBool(128, false) // sign bias altref

	// Refresh entropy probs.
	fp.writeBool(128, false)
	// Refresh last.
	fp.writeBool(128, true)

	// Coeff prob updates: none.
	for i := range DefaultCoeffProbs {
		for j := range DefaultCoeffProbs[i] {
			for k := range DefaultCoeffProbs[i][j] {
				for l := range DefaultCoeffProbs[i][j][k] {
					fp.writeBool(CoeffUpdateProbs[i][j][k][l], false)
				}
			}
		}
	}

	// Skip enabled.
	fp.writeBool(128, true)
	fp.writeLit(8, 1)

	// Inter-frame probs.
	fp.writeLit(8, 128) // prob_intra (128 = equal chance inter/intra)
	fp.writeLit(8, 128) // prob_last
	fp.writeLit(8, 128) // prob_golden
	// No Y mode prob update.
	fp.writeBool(128, false)
	// No UV mode prob update.
	fp.writeBool(128, false)
	// No MV prob updates.
	for i := 0; i < 2; i++ {
		for j := 0; j < 19; j++ {
			fp.writeBool(mvUpdateProbs[i][j], false)
		}
	}

	// Token partition.
	e.coeffParts.reset()
	coeffParts := e.coeffParts

	// Reconstruction buffer to track what the decoder produces.
	recon := e.reconBuf[:padW*padH]
	if e.lastY != nil {
		copy(recon, e.lastY)
	} else {
		for i := range recon {
			recon[i] = 0
		}
	}

	yDCQ := int(dcQLookup[qp])

	// NNZ tracking (must match decoder).
	topNNZY := make([]bool, 4*mbW)
	topNNZU := make([]bool, 2*mbW)
	topNNZV := make([]bool, 2*mbW)

	// Encode each MB: ZEROMV skip if identical to reference, else intra B_PRED.
	for mbY := 0; mbY < mbH; mbY++ {
		var leftNNZY [4]bool
		var leftNNZU [2]bool
		var leftNNZV [2]bool
		for mbX := 0; mbX < mbW; mbX++ {
			// Check if MB is identical to reference (ZEROMV skip candidate).
			mbIdentical := e.lastY != nil && mbBytesEqual(curY, e.lastY, padW, mbX, mbY)

			if mbIdentical {
				// ZEROMV skip: decoder copies reference as-is.
				fp.writeBool(1, false) // is_inter = true
				fp.writeBool(128, false) // ref = last
				fp.writeBool(162, false) // ZEROMV
				fp.writeBool(1, true) // skip = true
				// recon already has lastY content for this MB.
				continue
			}

			// Intra B_PRED for changed MBs.
			fp.writeBool(1, true) // is_intra = true

			// Write Y mode = B_PRED using inter-frame ymode tree.
			writeInterIntraYMode(fp, predBPRD)

			// Choose B_PRED modes and encode sub-block modes.
			modes, bpQCoeffs, mbAllZero := chooseBPredModes(curY, recon, padW, mbX, mbY, yDCQ, yACQ)
			for i := 0; i < 16; i++ {
				writeSubBlockMode(fp, modes[i], interBModeProbs[:])
			}

			// Write UV mode = DC_PRED.
			fp.writeBool(defaultUVModeProbs[0], false)

			// Skip flag.
			fp.writeBool(1, mbAllZero)

			if !mbAllZero {
				// Encode B_PRED sub-blocks with NNZ context (plane 3: DC+AC together).
				for by := 0; by < 4; by++ {
					for bx := 0; bx < 4; bx++ {
						i := by*4 + bx
						top := topNNZY[mbX*4+bx]
						left := leftNNZY[by]
						initCtx := boolInt(top) + boolInt(left)
						encodeBPredBlock(coeffParts, bpQCoeffs[i][:], initCtx)
						nz := hasNonZero(bpQCoeffs[i][:])
						topNNZY[mbX*4+bx] = nz
						leftNNZY[by] = nz
					}
				}
				// U/V blocks (zero residual) with NNZ context.
				for by := 0; by < 2; by++ {
					for bx := 0; bx < 2; bx++ {
						var zero [16]int16
						initCtx := boolInt(topNNZU[mbX*2+bx]) + boolInt(leftNNZU[by])
						encodeBlock(coeffParts, zero[:], 2, initCtx)
						topNNZU[mbX*2+bx] = false
						leftNNZU[by] = false
					}
				}
				for by := 0; by < 2; by++ {
					for bx := 0; bx < 2; bx++ {
						var zero [16]int16
						initCtx := boolInt(topNNZV[mbX*2+bx]) + boolInt(leftNNZV[by])
						encodeBlock(coeffParts, zero[:], 2, initCtx)
						topNNZV[mbX*2+bx] = false
						leftNNZV[by] = false
					}
				}
			} else {
				// Skip: clear NNZ for this MB.
				for bx := 0; bx < 4; bx++ {
					topNNZY[mbX*4+bx] = false
				}
				leftNNZY = [4]bool{}
				for bx := 0; bx < 2; bx++ {
					topNNZU[mbX*2+bx] = false
					topNNZV[mbX*2+bx] = false
				}
				leftNNZU = [2]bool{}
				leftNNZV = [2]bool{}
			}

			// Reconstruct B_PRED MB into recon buffer (done by chooseBPredModes).
		}
	}

	fp.flush()
	coeffParts.flush()

	// Store reconstructed frame as reference (must match decoder output).
	if cap(e.lastY) < len(recon) {
		e.lastY = make([]byte, len(recon))
	}
	e.lastY = e.lastY[:len(recon)]
	copy(e.lastY, recon)
	e.padW = padW
	e.padH = padH

	// Build inter-frame.
	return e.buildInterFrame(mbW, mbH, qp, fp.bytes(), coeffParts.bytes()), nil
}

// buildInterFrame assembles a VP8 inter-frame bitstream.
func (e *Encoder) buildInterFrame(mbW, mbH, qp int, firstPart, tokenPart []byte) []byte {
	_ = mbW
	_ = mbH
	_ = qp

	frameSize := 3 + len(firstPart) + len(tokenPart)
	out := make([]byte, 0, frameSize)

	// Frame tag: 3 bytes.
	// Bit 0: frame_type (1=inter)
	// Bits 1-2: version (0)
	// Bit 3: show_frame (1)
	// Bits 4+: first_part_size
	firstPartSize := uint32(len(firstPart))
	tag0 := byte(1) | (0 << 1) | (1 << 4) // inter, version 0, show=1
	tag0 |= byte((firstPartSize & 0x7) << 5)
	tag1 := byte((firstPartSize >> 3) & 0xFF)
	tag2 := byte((firstPartSize >> 11) & 0xFF)
	out = append(out, tag0, tag1, tag2)

	// No keyframe header for inter-frames.
	out = append(out, firstPart...)
	out = append(out, tokenPart...)
	return out
}

// buildKeyframe assembles the complete VP8 keyframe bitstream.
//
// VP8 frame assembly requires many sequential steps per spec.
func (e *Encoder) buildKeyframe(
	mbW, mbH, qp int,
	mbInfos []mbInfo,
	tokenPart []byte,
) []byte {
	w := e.width
	h := e.height

	// First partition: frame header + MB mode/skip data in one bool stream.
	fp := newBoolWriter()

	// Write constant header (same every frame for same qp).
	fp.writeBool(128, false) // YUV
	fp.writeBool(128, false) // no clamping
	fp.writeBool(128, false) // Segmentation disabled
	fp.writeBool(128, false) // normal filter
	fp.writeLit(6, 0)        // loop_filter_level
	fp.writeLit(3, 0)        // sharpness = 0
	fp.writeBool(128, false) // Loop filter adjustments disabled
	fp.writeLit(2, 0)        // Number of token partitions: 0
	fp.writeLit(7, uint32(qp))
	fp.writeBool(128, false) // y_dc_delta
	fp.writeBool(128, false) // y2_dc_delta
	fp.writeBool(128, false) // y2_ac_delta
	fp.writeBool(128, false) // uv_dc_delta
	fp.writeBool(128, false) // uv_ac_delta
	fp.writeBool(128, false) // Refresh entropy probs: no
	for i := range DefaultCoeffProbs {
		for j := range DefaultCoeffProbs[i] {
			for k := range DefaultCoeffProbs[i][j] {
				for l := range DefaultCoeffProbs[i][j][k] {
					fp.writeBool(CoeffUpdateProbs[i][j][k][l], false)
				}
			}
		}
	}
	fp.writeBool(128, true) // mb_no_coeff_skip = true
	fp.writeLit(8, 128)    // skip_prob

	// Macroblock data: mode + skip for each MB.
	// Maintain sub-block mode context across MBs per RFC 6386 section 11.3.
	aboveRow := make([][4]int, mbW) // bottom-row modes of each MB in the row above
	var leftCtx [4]int
	for mbY := 0; mbY < mbH; mbY++ {
		leftCtx = [4]int{} // reset left context at start of each row
		for mbX := 0; mbX < mbW; mbX++ {
			info := &mbInfos[mbY*mbW+mbX]
			// Skip flag before mode (VP8 spec).
			fp.writeBool(128, info.skip)
			writeKFYMode(fp, info.yMode)
			if info.yMode == predBPRD {
				// Fast path: if all sub-block modes are DC, write 16 DC bits directly.
				allDC := true
				for _, m := range info.bModes {
					if m != bpredDC {
						allDC = false
						break
					}
				}
				if allDC {
					// All modes are DC. For sub-blocks where both above and left
					// context are DC, use precomputed probability.
					dcDCProb := KfYModeProb[bpredDC][bpredDC][0]
					for si := 0; si < 16; si++ {
						sby := si / 4
						sbx := si % 4
						if sby > 0 && sbx > 0 {
							// Both above and left are DC (from this MB).
							fp.writeBool(dcDCProb, false)
						} else {
							var above, left int
							if sby > 0 {
								above = bpredDC
							} else {
								above = aboveRow[mbX][sbx]
							}
							if sbx > 0 {
								left = bpredDC
							} else {
								left = leftCtx[sby]
							}
							fp.writeBool(KfYModeProb[above][left][0], false)
						}
					}
				} else {
					writeKFBPredModes(fp, info.bModes, aboveRow[mbX], leftCtx)
				}
				// Update context: bottom row (sub-blocks 12,13,14,15)
				aboveRow[mbX] = [4]int{info.bModes[12], info.bModes[13], info.bModes[14], info.bModes[15]}
				// Update context: right column (sub-blocks 3,7,11,15)
				leftCtx = [4]int{info.bModes[3], info.bModes[7], info.bModes[11], info.bModes[15]}
			} else {
				bm := yModeToBMode(info.yMode)
				aboveRow[mbX] = [4]int{bm, bm, bm, bm}
				leftCtx = [4]int{bm, bm, bm, bm}
			}
			// UV mode = DC_PRED.
			fp.writeBool(defaultUVModeProbs[0], false)
		}
	}

	fp.flush()
	firstPartBytes := fp.bytes()

	// Build uncompressed frame tag (3 bytes for keyframe) + 7 bytes keyframe header.
	frameSize := 10 + len(firstPartBytes) + len(tokenPart)
	out := make([]byte, 0, frameSize)

	// Frame tag: 3 bytes.
	firstPartSize := uint32(len(firstPartBytes))
	tag0 := byte(0) | (0 << 1) | (1 << 4)
	tag0 |= byte((firstPartSize & 0x7) << 5)
	tag1 := byte((firstPartSize >> 3) & 0xFF)
	tag2 := byte((firstPartSize >> 11) & 0xFF)
	out = append(out, tag0, tag1, tag2, 0x9D, 0x01, 0x2A)
	var wb [2]byte
	binary.LittleEndian.PutUint16(wb[:], uint16(w&0x3FFF))
	out = append(out, wb[:]...)
	var hb [2]byte
	binary.LittleEndian.PutUint16(hb[:], uint16(h&0x3FFF))
	out = append(out, hb[:]...)

	out = append(out, firstPartBytes...)
	out = append(out, tokenPart...)

	return out
}

// writeKFYMode writes the keyframe Y prediction mode to the bool stream.
// VP8 kf_ymode_tree: B_PRED=0, DC=100, V=101, H=110, TM=111.
func writeKFYMode(bw *boolWriter, mode int) {
	switch mode {
	case predBPRD:
		bw.writeBool(defaultYModeProbs[0], false)
	case predDC:
		bw.writeBool(defaultYModeProbs[0], true)
		bw.writeBool(defaultYModeProbs[1], false)
		bw.writeBool(defaultYModeProbs[2], false)
	case predV:
		bw.writeBool(defaultYModeProbs[0], true)
		bw.writeBool(defaultYModeProbs[1], false)
		bw.writeBool(defaultYModeProbs[2], true)
	case predH:
		bw.writeBool(defaultYModeProbs[0], true)
		bw.writeBool(defaultYModeProbs[1], true)
		bw.writeBool(defaultYModeProbs[3], false)
	case predTM:
		bw.writeBool(defaultYModeProbs[0], true)
		bw.writeBool(defaultYModeProbs[1], true)
		bw.writeBool(defaultYModeProbs[3], true)
	}
}

// writeInterIntraYMode writes the inter-frame intra Y mode using the ymode_tree.
// Tree: DC=0, {V,H}=10/11, {TM,B_PRED}=110/111.
func writeInterIntraYMode(bw *boolWriter, mode int) {
	switch mode {
	case predDC:
		bw.writeBool(interYModeProbs[0], false)
	case predV:
		bw.writeBool(interYModeProbs[0], true)
		bw.writeBool(interYModeProbs[1], false)
		bw.writeBool(interYModeProbs[2], false)
	case predH:
		bw.writeBool(interYModeProbs[0], true)
		bw.writeBool(interYModeProbs[1], false)
		bw.writeBool(interYModeProbs[2], true)
	case predTM:
		bw.writeBool(interYModeProbs[0], true)
		bw.writeBool(interYModeProbs[1], true)
		bw.writeBool(interYModeProbs[3], false)
	case predBPRD:
		bw.writeBool(interYModeProbs[0], true)
		bw.writeBool(interYModeProbs[1], true)
		bw.writeBool(interYModeProbs[3], true)
	}
}

// encodeBlock encodes a quantized 4x4 block into the token partition.
// plane: 0=Y-after-Y2, 1=Y2, 2=UV, 3=Y-without-Y2.
func encodeBlock(bw *boolWriter, coeffs []int16, plane int, initCtx int) {
	// Find last non-zero coefficient.
	last := -1
	for i := 15; i >= 0; i-- {
		if coeffs[zigzag[i]] != 0 {
			last = i
			break
		}
	}

	startIdx := 0
	if plane == 0 {
		startIdx = 1
	}

	ctx := initCtx
	i := startIdx
	probs := DefaultCoeffProbs[plane][coeffBand(i)][ctx]

	// Wrapper EOB check for first coefficient.
	if i > last {
		bw.writeBool(probs[0], false) // EOB
		return
	}
	bw.writeBool(probs[0], true) // not EOB

	// Enter skip_eob loop.
	for {
		probs = DefaultCoeffProbs[plane][coeffBand(i)][ctx]
		v := coeffs[zigzag[i]]

		if v == 0 {
			bw.writeBool(probs[1], false) // zero
			ctx = 0
			i++
			if i >= 16 {
				return
			}
			// After zero: skip EOB for next coefficient.
			continue
		}

		// Non-zero.
		bw.writeBool(probs[1], true)
		absV := v
		if absV < 0 {
			absV = -absV
		}
		encodeTokenValue(bw, probs, absV)
		bw.writeBool(128, v < 0)

		if absV == 1 {
			ctx = 1
		} else {
			ctx = 2
		}
		i++
		if i >= 16 {
			return
		}
		// After non-zero: write EOB check for next coefficient.
		probs = DefaultCoeffProbs[plane][coeffBand(i)][ctx]
		if i > last {
			bw.writeBool(probs[0], false) // EOB
			return
		}
		bw.writeBool(probs[0], true) // not EOB
	}
}

// encodeTokenValue encodes the magnitude of a non-zero coefficient.
// The VP8 token tree (RFC 6386 section 13.2):
//
//	probs[2]: 1 vs >1
//	probs[3]: 2 vs >2
//	probs[4]: 3-4 vs >4
//	probs[5]: 3 vs 4
//	probs[6]: 5-6 vs >6
//	probs[7]: 5(CAT1) vs 6(CAT2 start)... actually 5-6 vs 7-10
//	probs[8]: CAT1-2 vs CAT3+
//	probs[9]: CAT3-4 vs CAT5-6
//	probs[10]: CAT5 vs CAT6
//
// Token value ranges:
//
//	1: literal 1
//	2: literal 2
//	3: literal 3
//	4: literal 4
//	5-6: CAT1 (base 5, 1 extra bit)
//	7-10: CAT2 (base 7, 2 extra bits)
//	11-18: CAT3 (base 11, 3 extra bits)
//	19-34: CAT4 (base 19, 4 extra bits)
//	35-66: CAT5 (base 35, 5 extra bits)
//	67+: CAT6 (base 67, 11 extra bits)
//
// encodeTokenValue encodes the magnitude of a non-zero coefficient.
// encodeTokenValue encodes a non-zero coefficient magnitude using the VP8
// token tree from RFC 6386 section 13.2 / libvpx vp8_coef_tree:
//
//	prob[2]: ONE   vs >1
//	prob[3]: 2-4   vs CAT1+
//	prob[4]: 2     vs 3-4
//	prob[5]: 3     vs 4
//	prob[6]: CAT1-2 vs CAT3+
//	prob[7]: CAT1   vs CAT2
//	prob[8]: CAT3-4 vs CAT5-6
//	prob[9]: CAT3   vs CAT4
//	prob[10]: CAT5  vs CAT6
func encodeTokenValue(bw *boolWriter, probs [11]uint8, absV int16) {
	if absV == 1 {
		bw.writeBool(probs[2], false) // == 1
		return
	}
	bw.writeBool(probs[2], true) // > 1

	if absV <= 4 {
		bw.writeBool(probs[3], false) // 2, 3, or 4
		if absV == 2 {
			bw.writeBool(probs[4], false) // == 2
			return
		}
		bw.writeBool(probs[4], true) // 3 or 4
		bw.writeBool(probs[5], absV == 4)
		return
	}
	bw.writeBool(probs[3], true) // CAT1+ (>4)

	if absV <= 10 {
		bw.writeBool(probs[6], false) // CAT1 or CAT2
		if absV <= 6 {
			bw.writeBool(probs[7], false) // CAT1: 5-6
			extra := absV - 5
			bw.writeBool(tokenExtraBits[5].prob[0], extra&1 != 0)
		} else {
			bw.writeBool(probs[7], true) // CAT2: 7-10
			extra := absV - 7
			for i, p := range tokenExtraBits[6].prob {
				bw.writeBool(p, (extra>>int16(1-i))&1 != 0)
			}
		}
		return
	}
	bw.writeBool(probs[6], true) // CAT3+

	if absV <= 34 {
		bw.writeBool(probs[8], false) // CAT3 or CAT4
		if absV <= 18 {
			bw.writeBool(probs[9], false) // CAT3: 11-18
			extra := absV - 11
			for i, p := range tokenExtraBits[7].prob {
				bw.writeBool(p, (extra>>int16(2-i))&1 != 0)
			}
		} else {
			bw.writeBool(probs[9], true) // CAT4: 19-34
			extra := absV - 19
			for i, p := range tokenExtraBits[8].prob {
				bw.writeBool(p, (extra>>int16(3-i))&1 != 0)
			}
		}
		return
	}
	bw.writeBool(probs[8], true) // CAT5+

	if absV <= 66 {
		bw.writeBool(probs[10], false) // CAT5: 35-66
		extra := absV - 35
		for i, p := range tokenExtraBits[9].prob {
			bw.writeBool(p, (extra>>int16(4-i))&1 != 0)
		}
	} else {
		bw.writeBool(probs[10], true) // CAT6: 67+
		extra := absV - 67
		for i, p := range tokenExtraBits[10].prob {
			bw.writeBool(p, (extra>>int16(10-i))&1 != 0)
		}
	}
}

// coeffBand returns the coefficient band for position i (0-15).
func coeffBand(i int) int {
	// VP8 coefficient bands from RFC 6386.
	bands := [16]int{0, 1, 2, 3, 6, 4, 5, 6, 6, 6, 6, 6, 6, 6, 6, 7}
	return bands[i]
}

func quantize(val int16, q int) int16 {
	if val == 0 {
		return 0
	}
	sign := int16(1)
	v := int(val)
	if v < 0 {
		sign = -1
		v = -v
	}
	r := (v + q/2) / q
	return int16(r) * sign
}

// boolInt converts a bool to 0 or 1.
func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// hasNonZero returns true if any coefficient in the block is non-zero.
func hasNonZero(coeffs []int16) bool {
	for _, c := range coeffs {
		if c != 0 {
			return true
		}
	}
	return false
}

func padFrameInto(dst, src []byte, srcW, srcH, dstW, dstH int) []byte {
	// Fast path: copy source rows directly, extend last column.
	for row := 0; row < srcH; row++ {
		copy(dst[row*dstW:], src[row*srcW:row*srcW+srcW])
		if dstW > srcW {
			last := src[row*srcW+srcW-1]
			for col := srcW; col < dstW; col++ {
				dst[row*dstW+col] = last
			}
		}
	}
	// Extend last row downward.
	if dstH > srcH {
		lastRow := dst[(srcH-1)*dstW : (srcH-1)*dstW+dstW]
		for row := srcH; row < dstH; row++ {
			copy(dst[row*dstW:], lastRow)
		}
	}
	return dst
}

func clampQP(v int) int {
	if v < 0 {
		return 0
	}
	if v > 127 {
		return 127
	}
	return v
}

// mbBytesEqual checks if a 16x16 MB in cur is byte-identical to ref.
func mbBytesEqual(cur, ref []byte, stride, mbX, mbY int) bool {
	ox := mbX * 16
	oy := mbY * 16
	for row := 0; row < 16; row++ {
		off := (oy+row)*stride + ox
		for col := 0; col < 16; col++ {
			if cur[off+col] != ref[off+col] {
				return false
			}
		}
	}
	return true
}

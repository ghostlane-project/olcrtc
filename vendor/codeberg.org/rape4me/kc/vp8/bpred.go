package vp8

// B_PRED encoding/decoding support for VP8 keyframes.
// In B_PRED mode, each 4x4 sub-block has its own intra prediction mode,
// and residuals are encoded per-block (no Y2 WHT).

// yModeToBMode converts a 16x16 Y prediction mode to the equivalent sub-block mode
// for context purposes (RFC 6386 section 11.3 point 4).
func yModeToBMode(yMode int) int {
	switch yMode {
	case predDC:
		return bpredDC
	case predV:
		return bpredVE
	case predH:
		return bpredHE
	case predTM:
		return bpredTM
	default:
		return bpredDC
	}
}

// writeKFBPredModes writes 16 sub-block modes using the KfYModeProb tree.
// Per RFC 6386 section 11.3, each sub-block mode is coded conditioned on
// the above and left sub-block modes, which may come from neighboring MBs.
// aboveCtx: 4 modes from the bottom row of the MB above (or B_DC_PRED if none).
// leftCtx: 4 modes from the right column of the MB to the left (or B_DC_PRED if none).
func writeKFBPredModes(bw *boolWriter, modes [16]int, aboveCtx [4]int, leftCtx [4]int) {
	for i := 0; i < 16; i++ {
		by := i / 4
		bx := i % 4
		var above, left int
		if by > 0 {
			above = modes[(by-1)*4+bx]
		} else {
			above = aboveCtx[bx]
		}
		if bx > 0 {
			left = modes[by*4+bx-1]
		} else {
			left = leftCtx[by]
		}
		writeSubBlockMode(bw, modes[i], KfYModeProb[above][left][:])
	}
}

// writeSubBlockMode writes a single sub-block mode using the bmode_tree.
// Tree structure from RFC 6386:
//
//	prob[0]: DC vs rest
//	prob[1]: TM vs rest
//	prob[2]: VE vs rest
//	prob[3]: {HE,RD,VR} vs {LD,VL,HD,HU}
//	prob[4]: HE vs {RD,VR}
//	prob[5]: RD vs VR
//	prob[6]: LD vs {VL,HD,HU}
//	prob[7]: VL vs {HD,HU}
//	prob[8]: HD vs HU
func writeSubBlockMode(bw *boolWriter, mode int, probs []uint8) {
	switch mode {
	case bpredDC:
		bw.writeBool(probs[0], false)
	case bpredTM:
		bw.writeBool(probs[0], true)
		bw.writeBool(probs[1], false)
	case bpredVE:
		bw.writeBool(probs[0], true)
		bw.writeBool(probs[1], true)
		bw.writeBool(probs[2], false)
	case bpredHE:
		bw.writeBool(probs[0], true)
		bw.writeBool(probs[1], true)
		bw.writeBool(probs[2], true)
		bw.writeBool(probs[3], false)
		bw.writeBool(probs[4], false)
	case bpredRD:
		bw.writeBool(probs[0], true)
		bw.writeBool(probs[1], true)
		bw.writeBool(probs[2], true)
		bw.writeBool(probs[3], false)
		bw.writeBool(probs[4], true)
		bw.writeBool(probs[5], false)
	case bpredVR:
		bw.writeBool(probs[0], true)
		bw.writeBool(probs[1], true)
		bw.writeBool(probs[2], true)
		bw.writeBool(probs[3], false)
		bw.writeBool(probs[4], true)
		bw.writeBool(probs[5], true)
	case bpredLD:
		bw.writeBool(probs[0], true)
		bw.writeBool(probs[1], true)
		bw.writeBool(probs[2], true)
		bw.writeBool(probs[3], true)
		bw.writeBool(probs[6], false)
	case bpredVL:
		bw.writeBool(probs[0], true)
		bw.writeBool(probs[1], true)
		bw.writeBool(probs[2], true)
		bw.writeBool(probs[3], true)
		bw.writeBool(probs[6], true)
		bw.writeBool(probs[7], false)
	case bpredHD:
		bw.writeBool(probs[0], true)
		bw.writeBool(probs[1], true)
		bw.writeBool(probs[2], true)
		bw.writeBool(probs[3], true)
		bw.writeBool(probs[6], true)
		bw.writeBool(probs[7], true)
		bw.writeBool(probs[8], false)
	case bpredHU:
		bw.writeBool(probs[0], true)
		bw.writeBool(probs[1], true)
		bw.writeBool(probs[2], true)
		bw.writeBool(probs[3], true)
		bw.writeBool(probs[6], true)
		bw.writeBool(probs[7], true)
		bw.writeBool(probs[8], true)
	}
}

// readKFSubBlockMode reads a single sub-block mode from the bitstream.
func readKFSubBlockMode(br *boolReader, probs []uint8) int {
	if !br.readBool(probs[0]) {
		return bpredDC
	}
	if !br.readBool(probs[1]) {
		return bpredTM
	}
	if !br.readBool(probs[2]) {
		return bpredVE
	}
	if !br.readBool(probs[3]) {
		// Left subtree: HE, RD, VR
		if !br.readBool(probs[4]) {
			return bpredHE
		}
		if !br.readBool(probs[5]) {
			return bpredRD
		}
		return bpredVR
	}
	// Right subtree: LD, VL, HD, HU
	if !br.readBool(probs[6]) {
		return bpredLD
	}
	if !br.readBool(probs[7]) {
		return bpredVL
	}
	if !br.readBool(probs[8]) {
		return bpredHD
	}
	return bpredHU
}

// readKFBPredModes reads 16 sub-block modes for a B_PRED macroblock.
// aboveCtx: 4 modes from the bottom row of the MB above.
// leftCtx: 4 modes from the right column of the MB to the left.
func readKFBPredModes(br *boolReader, aboveCtx [4]int, leftCtx [4]int) [16]int {
	var modes [16]int
	for i := 0; i < 16; i++ {
		by := i / 4
		bx := i % 4
		var above, left int
		if by > 0 {
			above = modes[(by-1)*4+bx]
		} else {
			above = aboveCtx[bx]
		}
		if bx > 0 {
			left = modes[by*4+bx-1]
		} else {
			left = leftCtx[by]
		}
		modes[i] = readKFSubBlockMode(br, KfYModeProb[above][left][:])
	}
	return modes
}

// reconError4x4 computes the sum of absolute reconstruction error for a 4x4 block
// after quantize/dequantize round-trip.
func reconError4x4(src []byte, srcStride, srcX, srcY int, pred []byte, yDCQ, yACQ int) int {
	// Fast path: if all residuals are zero, reconstruction error is zero.
	allZero := true
	for row := 0; row < 4; row++ {
		off := (srcY+row)*srcStride + srcX
		for col := 0; col < 4; col++ {
			if src[off+col] != pred[row*4+col] {
				allZero = false
				goto full
			}
		}
	}
full:
	if allZero {
		return 0
	}

	var block [16]int16
	for row := 0; row < 4; row++ {
		off := (srcY+row)*srcStride + srcX
		for col := 0; col < 4; col++ {
			block[row*4+col] = int16(src[off+col]) - int16(pred[row*4+col])
		}
	}
	dct := fdct4x4(block)
	// Quantize then dequantize.
	var dq [16]int16
	dq[0] = quantize(dct[0], yDCQ) * int16(yDCQ)
	for i := 1; i < 16; i++ {
		dq[i] = quantize(dct[i], yACQ) * int16(yACQ)
	}
	residual := idct4x4(dq)
	// Compute reconstruction error.
	err := 0
	for row := 0; row < 4; row++ {
		off := (srcY+row)*srcStride + srcX
		for col := 0; col < 4; col++ {
			recon := int(pred[row*4+col]) + int(residual[row*4+col])
			if recon < 0 {
				recon = 0
			} else if recon > 255 {
				recon = 255
			}
			d := int(src[off+col]) - recon
			if d < 0 {
				d = -d
			}
			err += d
		}
	}
	return err
}

// chooseBPredModes selects the best sub-block mode for each 4x4 block in a MB.
// It performs prediction into a reconstructed buffer so subsequent blocks can
// reference the already-predicted pixels. Returns modes and quantized coefficients.
// `ctx` is the reconstruction buffer used for border context (above/left from neighboring MBs).
// `src` is the original source frame used for computing residuals.
//
//nolint:funlen,cyclop,gocognit,gocyclo,maintidx // VP8 B_PRED mode selection per RFC 6386.
func chooseBPredModes(
	src []byte, ctx []byte, stride int, mbX, mbY int,
	yDCQ, yACQ int,
) (modes [16]int, qcoeffs [16][16]int16, allZero bool) {
	// Fast path: if the entire 16x16 block and its border context are uniform,
	// DC prediction is perfect for all sub-blocks with zero coefficients.
	srcBase := mbY*16*stride + mbX*16
	val := src[srcBase]
	uniform := true
	for row := 0; row < 16; row++ {
		off := srcBase + row*stride
		for col := 0; col < 16; col++ {
			if src[off+col] != val {
				uniform = false
				goto slowPath
			}
		}
	}
	if uniform {
		// Check border: above row must all be val (or match DC prediction expectation).
		borderMatch := true
		if mbY > 0 {
			aboveRow := (mbY*16-1)*stride + mbX*16
			for col := 0; col < 16; col++ {
				if ctx[aboveRow+col] != val {
					borderMatch = false
					break
				}
			}
		} else {
			// Top edge: DC uses only left or defaults to 128/127.
			if mbX > 0 {
				// DC will use left only. Check left column.
				leftCol := mbX*16 - 1
				for row := 0; row < 16; row++ {
					if ctx[(mbY*16+row)*stride+leftCol] != val {
						borderMatch = false
						break
					}
				}
			} else {
				// Corner MB: DC defaults to 128. Only uniform if val==128.
				borderMatch = (val == 128)
			}
		}
		if borderMatch && mbX > 0 {
			leftCol := mbX*16 - 1
			for row := 0; row < 16; row++ {
				if ctx[(mbY*16+row)*stride+leftCol] != val {
					borderMatch = false
					break
				}
			}
		}
		if borderMatch {
			// All modes are DC with zero coefficients. Recon already correct from pre-copy.
			// modes is already zero-initialized (bpredDC=0), qcoeffs already zero.
			return modes, qcoeffs, true
		}
	}
slowPath:
	// Process sub-blocks using ctx (recon buffer) directly for context.
	// For edge MBs, we need special handling of border values.
	allZero = true
	for i := 0; i < 16; i++ {
		by := i / 4
		bx := i % 4
		srcX := mbX*16 + bx*4
		srcY := mbY*16 + by*4

		// Compute DC prediction from above and left context.
		var dcSum int
		if srcY > 0 {
			aboveOff := (srcY-1)*stride + srcX
			dcSum += int(ctx[aboveOff]) + int(ctx[aboveOff+1]) + int(ctx[aboveOff+2]) + int(ctx[aboveOff+3])
		} else {
			dcSum += 127 * 4
		}
		if srcX > 0 {
			leftOff := srcY*stride + srcX - 1
			dcSum += int(ctx[leftOff]) + int(ctx[leftOff+stride]) + int(ctx[leftOff+2*stride]) + int(ctx[leftOff+3*stride])
		} else {
			dcSum += 129 * 4
		}
		dc := byte((dcSum + 4) >> 3)

		// Check if DC prediction matches source exactly.
		off0 := srcY*stride + srcX
		off1 := off0 + stride
		off2 := off1 + stride
		off3 := off2 + stride
		if src[off0] == dc && src[off0+1] == dc && src[off0+2] == dc && src[off0+3] == dc &&
			src[off1] == dc && src[off1+1] == dc && src[off1+2] == dc && src[off1+3] == dc &&
			src[off2] == dc && src[off2+1] == dc && src[off2+2] == dc && src[off2+3] == dc &&
			src[off3] == dc && src[off3+1] == dc && src[off3+2] == dc && src[off3+3] == dc {
			// Perfect DC prediction - recon already correct from pre-copy.
			continue
		}

		// DC prediction not perfect - compute coefficients.
		allZero = false
		var block [16]int16
		for row := 0; row < 4; row++ {
			off := (srcY+row)*stride + srcX
			block[row*4] = int16(src[off]) - int16(dc)
			block[row*4+1] = int16(src[off+1]) - int16(dc)
			block[row*4+2] = int16(src[off+2]) - int16(dc)
			block[row*4+3] = int16(src[off+3]) - int16(dc)
		}
		dct := fdct4x4(block)
		qcoeffs[i][0] = quantize(dct[0], yDCQ)
		for j := 1; j < 16; j++ {
			qcoeffs[i][j] = quantize(dct[j], yACQ)
		}
		// Dequantize for reconstruction.
		var dqcoeffs [16]int16
		dqcoeffs[0] = qcoeffs[i][0] * int16(yDCQ)
		for j := 1; j < 16; j++ {
			dqcoeffs[j] = qcoeffs[i][j] * int16(yACQ)
		}
		residual := idct4x4(dqcoeffs)

		// Store reconstructed pixels in recon buffer.
		for row := 0; row < 4; row++ {
			off := (srcY+row)*stride + srcX
			for col := 0; col < 4; col++ {
				v := int(dc) + int(residual[row*4+col])
				if v < 0 {
					v = 0
				} else if v > 255 {
					v = 255
				}
				ctx[off+col] = byte(v)
			}
		}
	}
	return modes, qcoeffs, allZero
}

// encodeBPredBlock encodes a full 4x4 block (DC+AC) for B_PRED mode.
// Unlike normal Y sub-blocks which skip DC (sent via Y2), B_PRED sends DC inline.
// initCtx is the initial coefficient context (0..2) based on neighboring blocks.
func encodeBPredBlock(bw *boolWriter, coeffs []int16, initCtx int) {
	last := -1
	for i := 15; i >= 0; i-- {
		if coeffs[zigzag[i]] != 0 {
			last = i
			break
		}
	}

	ctx := initCtx
	i := 0
	probs := DefaultCoeffProbs[3][coeffBand(i)][ctx]

	if i > last {
		bw.writeBool(probs[0], false)
		return
	}
	bw.writeBool(probs[0], true)

	for {
		probs = DefaultCoeffProbs[3][coeffBand(i)][ctx]
		v := coeffs[zigzag[i]]

		if v == 0 {
			bw.writeBool(probs[1], false)
			ctx = 0
			i++
			if i >= 16 {
				return
			}
			continue
		}

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
		probs = DefaultCoeffProbs[3][coeffBand(i)][ctx]
		if i > last {
			bw.writeBool(probs[0], false)
			return
		}
		bw.writeBool(probs[0], true)
	}
}

// decodeBPredBlock decodes a full 4x4 block (DC+AC) for B_PRED mode.
// Unlike normal Y sub-blocks which skip DC, B_PRED includes DC at position 0.
// Returns true if the block has any non-zero coefficient.
func decodeBPredBlock(br *boolReader, coeffs []int16, probs *[4][8][3][11]uint8, initCtx int) bool {
	ctx := initCtx
	i := 0
	p := probs[3][coeffBand(i)][ctx]
	if !br.readBool(p[0]) {
		return false
	}
	hasNZ := false
	for {
		p = probs[3][coeffBand(i)][ctx]
		if !br.readBool(p[1]) {
			ctx = 0
			i++
			if i >= 16 {
				return hasNZ
			}
			continue
		}
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
		p = probs[3][coeffBand(i)][ctx]
		if !br.readBool(p[0]) {
			return hasNZ
		}
	}
}

// getDecoderSubBlockContext extracts above/left/aboveLeft from the reconstructed frame
// for a sub-block during decoding.
func getDecoderSubBlockContext(y []byte, yStride, mbX, mbY, bx, by int, above []byte, left []byte, al *byte) {
	px := mbX*16 + bx*4
	py := mbY*16 + by*4

	// Above: 8 pixels (4 directly above + 4 above-right).
	if py > 0 {
		for col := 0; col < 8; col++ {
			c := px + col
			if c >= yStride {
				c = yStride - 1
			}
			above[col] = y[(py-1)*yStride+c]
		}
	} else {
		for col := range above {
			above[col] = 127
		}
	}

	// Left: 4 pixels.
	if px > 0 {
		for row := 0; row < 4; row++ {
			left[row] = y[(py+row)*yStride+px-1]
		}
	} else {
		for row := range left {
			left[row] = 129
		}
	}

	// Above-left.
	if px > 0 && py > 0 {
		*al = y[(py-1)*yStride+px-1]
	} else if py > 0 && px == 0 {
		*al = 129 // left edge: pixel at (py-1, -1) is in "left" region
	} else if py == 0 && px > 0 {
		*al = 127 // top edge: pixel at (-1, px-1) is in "above" region
	} else {
		*al = 128
	}
}

// reconstructBPredMB writes reconstructed pixels for a B_PRED MB into the recon buffer.
// This mirrors what the decoder would produce, ensuring consistent context for subsequent MBs.
//
//nolint:cyclop,gocognit,gocyclo // VP8 B_PRED reconstruction per RFC 6386.
func reconstructBPredMB(recon []byte, stride, mbX, mbY int, modes [16]int, qcoeffs [16][16]int16, yDCQ, yACQ int) {
	var local [20 * 20]byte
	const ls = 20

	// Fill border from recon buffer - must match decoder's getDecoderSubBlockContext.
	if mbX > 0 && mbY > 0 {
		local[3*ls+3] = recon[(mbY*16-1)*stride+mbX*16-1]
	} else if mbY > 0 {
		local[3*ls+3] = 127
	} else if mbX > 0 {
		local[3*ls+3] = 129
	} else {
		local[3*ls+3] = 128
	}

	if mbY > 0 {
		srcRow := mbY*16 - 1
		srcCol := mbX * 16
		for col := 0; col < 16; col++ {
			local[3*ls+4+col] = recon[srcRow*stride+srcCol+col]
		}
		for col := 16; col < 20; col++ {
			if srcCol+col < stride {
				local[3*ls+4+col] = recon[srcRow*stride+srcCol+col]
			} else {
				local[3*ls+4+col] = recon[srcRow*stride+stride-1]
			}
		}
	} else {
		for col := 3; col < 20; col++ {
			local[3*ls+col] = 127
		}
		if mbX > 0 {
			local[3*ls+3] = 129
		} else {
			local[3*ls+3] = 128
		}
	}

	if mbX > 0 {
		srcCol := mbX*16 - 1
		for row := 0; row < 16; row++ {
			local[(4+row)*ls+3] = recon[(mbY*16+row)*stride+srcCol]
		}
	} else {
		for row := 0; row < 16; row++ {
			local[(4+row)*ls+3] = 129
		}
	}

	for i := 0; i < 16; i++ {
		by := i / 4
		bx := i % 4
		lx := 4 + bx*4
		ly := 4 + by*4

		var above [8]byte
		for col := 0; col < 8; col++ {
			above[col] = local[(ly-1)*ls+lx+col]
		}
		var left [4]byte
		for row := 0; row < 4; row++ {
			left[row] = local[(ly+row)*ls+lx-1]
		}
		al := local[(ly-1)*ls+lx-1]

		var pred [16]byte
		predict4x4(pred[:], 4, modes[i], above[:], left[:], al)

		// Dequantize.
		var dqcoeffs [16]int16
		dqcoeffs[0] = qcoeffs[i][0] * int16(yDCQ)
		for j := 1; j < 16; j++ {
			dqcoeffs[j] = qcoeffs[i][j] * int16(yACQ)
		}
		residual := idct4x4(dqcoeffs)

		// Store in local and recon.
		for row := 0; row < 4; row++ {
			for col := 0; col < 4; col++ {
				v := int(pred[row*4+col]) + int(residual[row*4+col])
				if v < 0 {
					v = 0
				} else if v > 255 {
					v = 255
				}
				local[(ly+row)*ls+lx+col] = byte(v)
				off := (mbY*16+by*4+row)*stride + mbX*16 + bx*4 + col
				recon[off] = byte(v)
			}
		}
	}
}

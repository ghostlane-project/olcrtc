package vp8

// Motion vector for inter prediction.
type mv struct {
	x, y int16 // quarter-pixel precision
}

// Default MV probabilities from RFC 6386 section 17.2.
//
//nolint:gochecknoglobals // RFC 6386 spec data.
var defaultMVProbs = [2][19]uint8{
	{162, 128, 225, 146, 172, 147, 214, 39, 156, 128, 129, 132, 75, 145, 178, 206, 239, 254, 254},
	{164, 128, 204, 170, 119, 235, 140, 230, 228, 128, 130, 130, 74, 148, 180, 203, 236, 254, 254},
}

// MV component probability indices.
const (
	mvIsShort  = 0
	mvSign     = 1
	mvShort    = 2 // 3 bits for short MV (0-7)
	mvBits     = 9 // long MV bits start
	mvBitsLong = 10
)

// readMV reads a motion vector component from the bool reader.
func readMVComponent(br *boolReader, probs [19]uint8) int16 {
	if !br.readBool(probs[mvIsShort]) {
		// Short MV (magnitude 0-7).
		var mag int16
		// Tree-coded 3-bit value.
		if !br.readBool(probs[mvShort]) {
			mag = 0
		} else if !br.readBool(probs[mvShort+1]) {
			mag = 1
		} else if !br.readBool(probs[mvShort+2]) {
			mag = 2
		} else if !br.readBool(probs[mvShort+3]) {
			mag = 3
		} else if !br.readBool(probs[mvShort+4]) {
			mag = 4
		} else if !br.readBool(probs[mvShort+5]) {
			mag = 5
		} else if !br.readBool(probs[mvShort+6]) {
			mag = 6
		} else {
			mag = 7
		}
		if mag != 0 && br.readBool(probs[mvSign]) {
			return -mag
		}
		return mag
	}
	// Long MV (magnitude 8+).
	var mag int16
	// Read 3 high bits with individual probs.
	for i := 0; i < 3; i++ {
		mag <<= 1
		if br.readBool(probs[mvBits+i]) {
			mag |= 1
		}
	}
	// Read 7 low bits with individual probs.
	for i := 3; i < 10; i++ {
		mag <<= 1
		if br.readBool(probs[mvBits+i]) {
			mag |= 1
		}
	}
	mag += 8
	if br.readBool(probs[mvSign]) {
		return -mag
	}
	return mag
}

// readMV reads a full motion vector (row, col).
func readMV(br *boolReader, mvProbs *[2][19]uint8) mv {
	row := readMVComponent(br, mvProbs[0])
	col := readMVComponent(br, mvProbs[1])
	return mv{x: col, y: row}
}

// Inter prediction: copy from reference frame with sub-pixel interpolation.
// VP8 uses 6-tap filter for half-pixel and bilinear for quarter-pixel.

// 6-tap filter coefficients for half-pixel positions.
//
// interPredBlock copies a block from ref with sub-pixel motion vector.
//
//nolint:gochecknoglobals // RFC 6386 spec data.
func interPredBlock(
	dst []byte, dstStride int,
	ref []byte, refStride int,
	refW, refH int,
	blockW, blockH int,
	mvX, mvY int16,
	dstX, dstY int,
) {
	// Integer and fractional parts of MV (quarter-pixel).
	intX := int(mvX) >> 2
	intY := int(mvY) >> 2
	fracX := int(mvX) & 3
	fracY := int(mvY) & 3

	srcX := dstX + intX
	srcY := dstY + intY

	// For simplicity, use bilinear interpolation for all sub-pixel positions.
	for row := 0; row < blockH; row++ {
		for col := 0; col < blockW; col++ {
			sx := srcX + col
			sy := srcY + row

			// Clamp to reference frame bounds.
			p00 := refPixel(ref, refStride, refW, refH, sx, sy)
			p10 := refPixel(ref, refStride, refW, refH, sx+1, sy)
			p01 := refPixel(ref, refStride, refW, refH, sx, sy+1)
			p11 := refPixel(ref, refStride, refW, refH, sx+1, sy+1)

			// Bilinear interpolation.
			fx := fracX * 2 // convert to eighths
			fy := fracY * 2
			val := (8-fx)*(8-fy)*int(p00) +
				fx*(8-fy)*int(p10) +
				(8-fx)*fy*int(p01) +
				fx*fy*int(p11)
			dst[(dstY+row)*dstStride+dstX+col] = byte((val + 32) >> 6)
		}
	}
}

func refPixel(ref []byte, stride, w, h, x, y int) byte {
	if x < 0 {
		x = 0
	} else if x >= w {
		x = w - 1
	}
	if y < 0 {
		y = 0
	} else if y >= h {
		y = h - 1
	}
	return ref[y*stride+x]
}

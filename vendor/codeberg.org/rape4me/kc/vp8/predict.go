package vp8

// Intra prediction modes for 16x16 luma (Y) macroblocks.
const (
	predDC   = 0
	predV    = 1
	predH    = 2
	predTM   = 3
	predBPRD = 4
)

// Sub-block intra prediction modes (4x4) for B_PRED.
const (
	bpredDC = 0
	bpredTM = 1
	bpredVE = 2
	bpredHE = 3
	bpredLD = 4
	bpredRD = 5
	bpredVR = 6
	bpredVL = 7
	bpredHD = 8
	bpredHU = 9
)

// predictY16 fills a 16x16 block with the predicted values for the given mode.
// above: 16 pixels above the block (or nil if top row).
// left: 16 pixels to the left (or nil if left column).
// aboveLeft: the pixel at top-left corner.
func predictY16(dst []byte, stride int, mode int, above []byte, left []byte, aboveLeft byte) {
	switch mode {
	case predDC:
		predictDC16(dst, stride, above, left)
	case predV:
		predictV16(dst, stride, above)
	case predH:
		predictH16(dst, stride, left)
	case predTM:
		predictTM16(dst, stride, above, left, aboveLeft)
	}
}

func predictDC16(dst []byte, stride int, above []byte, left []byte) {
	sum := 0
	count := 0
	if above != nil {
		for i := 0; i < 16; i++ {
			sum += int(above[i])
		}
		count += 16
	}
	if left != nil {
		for i := 0; i < 16; i++ {
			sum += int(left[i])
		}
		count += 16
	}
	var dc byte
	if count > 0 {
		dc = byte((sum + count/2) / count)
	} else {
		dc = 128
	}
	for row := 0; row < 16; row++ {
		for col := 0; col < 16; col++ {
			dst[row*stride+col] = dc
		}
	}
}

func predictV16(dst []byte, stride int, above []byte) {
	if above == nil {
		for i := 0; i < 16*stride; i += stride {
			for col := 0; col < 16; col++ {
				dst[i+col] = 127
			}
		}
		return
	}
	for row := 0; row < 16; row++ {
		copy(dst[row*stride:row*stride+16], above[:16])
	}
}

func predictH16(dst []byte, stride int, left []byte) {
	for row := 0; row < 16; row++ {
		var val byte
		if left != nil {
			val = left[row]
		} else {
			val = 129
		}
		for col := 0; col < 16; col++ {
			dst[row*stride+col] = val
		}
	}
}

func predictTM16(dst []byte, stride int, above []byte, left []byte, aboveLeft byte) {
	for row := 0; row < 16; row++ {
		var l int
		if left != nil {
			l = int(left[row])
		} else {
			l = 129
		}
		for col := 0; col < 16; col++ {
			var a int
			if above != nil {
				a = int(above[col])
			} else {
				a = 127
			}
			val := l + a - int(aboveLeft)
			dst[row*stride+col] = clampByte(val)
		}
	}
}

// predictUV8 fills an 8x8 chroma block.
func predictUV8(dst []byte, stride int, mode int, above []byte, left []byte, aboveLeft byte) {
	switch mode {
	case predDC:
		predictDC8(dst, stride, above, left)
	case predV:
		predictV8(dst, stride, above)
	case predH:
		predictH8(dst, stride, left)
	case predTM:
		predictTM8(dst, stride, above, left, aboveLeft)
	}
}

func predictDC8(dst []byte, stride int, above []byte, left []byte) {
	sum := 0
	count := 0
	if above != nil {
		for i := 0; i < 8; i++ {
			sum += int(above[i])
		}
		count += 8
	}
	if left != nil {
		for i := 0; i < 8; i++ {
			sum += int(left[i])
		}
		count += 8
	}
	var dc byte
	if count > 0 {
		dc = byte((sum + count/2) / count)
	} else {
		dc = 128
	}
	for row := 0; row < 8; row++ {
		for col := 0; col < 8; col++ {
			dst[row*stride+col] = dc
		}
	}
}

func predictV8(dst []byte, stride int, above []byte) {
	if above == nil {
		for row := 0; row < 8; row++ {
			for col := 0; col < 8; col++ {
				dst[row*stride+col] = 127
			}
		}
		return
	}
	for row := 0; row < 8; row++ {
		copy(dst[row*stride:row*stride+8], above[:8])
	}
}

func predictH8(dst []byte, stride int, left []byte) {
	for row := 0; row < 8; row++ {
		var val byte
		if left != nil {
			val = left[row]
		} else {
			val = 129
		}
		for col := 0; col < 8; col++ {
			dst[row*stride+col] = val
		}
	}
}

func predictTM8(dst []byte, stride int, above []byte, left []byte, aboveLeft byte) {
	for row := 0; row < 8; row++ {
		var l int
		if left != nil {
			l = int(left[row])
		} else {
			l = 129
		}
		for col := 0; col < 8; col++ {
			var a int
			if above != nil {
				a = int(above[col])
			} else {
				a = 127
			}
			val := l + a - int(aboveLeft)
			dst[row*stride+col] = clampByte(val)
		}
	}
}

// predict4x4 fills a 4x4 sub-block for B_PRED mode.
// above: 8 pixels (4 above + 4 above-right). left: 4 pixels. aboveLeft: 1 pixel.
func predict4x4(dst []byte, stride int, mode int, above []byte, left []byte, aboveLeft byte) {
	switch mode {
	case bpredDC:
		predict4DC(dst, stride, above, left)
	case bpredTM:
		predict4TM(dst, stride, above, left, aboveLeft)
	case bpredVE:
		predict4VE(dst, stride, above, aboveLeft)
	case bpredHE:
		predict4HE(dst, stride, left, aboveLeft)
	case bpredLD:
		predict4LD(dst, stride, above)
	case bpredRD:
		predict4RD(dst, stride, above, left, aboveLeft)
	case bpredVR:
		predict4VR(dst, stride, above, left, aboveLeft)
	case bpredVL:
		predict4VL(dst, stride, above)
	case bpredHD:
		predict4HD(dst, stride, above, left, aboveLeft)
	case bpredHU:
		predict4HU(dst, stride, left)
	}
}

func predict4DC(dst []byte, stride int, above []byte, left []byte) {
	sum := 0
	count := 0
	if above != nil {
		for i := 0; i < 4; i++ {
			sum += int(above[i])
		}
		count += 4
	}
	if left != nil {
		for i := 0; i < 4; i++ {
			sum += int(left[i])
		}
		count += 4
	}
	var dc byte
	if count > 0 {
		dc = byte((sum + count/2) / count)
	} else {
		dc = 128
	}
	for row := 0; row < 4; row++ {
		for col := 0; col < 4; col++ {
			dst[row*stride+col] = dc
		}
	}
}

func predict4TM(dst []byte, stride int, above []byte, left []byte, al byte) {
	for row := 0; row < 4; row++ {
		var l int
		if left != nil {
			l = int(left[row])
		} else {
			l = 129
		}
		for col := 0; col < 4; col++ {
			var a int
			if above != nil {
				a = int(above[col])
			} else {
				a = 127
			}
			dst[row*stride+col] = clampByte(l + a - int(al))
		}
	}
}

func avg3(a, b, c byte) byte {
	return byte((int(a) + 2*int(b) + int(c) + 2) >> 2)
}

func avg2(a, b byte) byte {
	return byte((int(a) + int(b) + 1) >> 1)
}

func predict4VE(dst []byte, stride int, above []byte, al byte) {
	if above == nil {
		for row := 0; row < 4; row++ {
			for col := 0; col < 4; col++ {
				dst[row*stride+col] = 127
			}
		}
		return
	}
	var pred [4]byte
	pred[0] = avg3(al, above[0], above[1])
	pred[1] = avg3(above[0], above[1], above[2])
	pred[2] = avg3(above[1], above[2], above[3])
	pred[3] = avg3(above[2], above[3], safeIdx(above, 4))
	for row := 0; row < 4; row++ {
		copy(dst[row*stride:row*stride+4], pred[:])
	}
}

func predict4HE(dst []byte, stride int, left []byte, al byte) {
	if left == nil {
		for row := 0; row < 4; row++ {
			for col := 0; col < 4; col++ {
				dst[row*stride+col] = 129
			}
		}
		return
	}
	vals := [4]byte{
		avg3(al, left[0], left[1]),
		avg3(left[0], left[1], left[2]),
		avg3(left[1], left[2], left[3]),
		avg3(left[2], left[3], left[3]),
	}
	for row := 0; row < 4; row++ {
		for col := 0; col < 4; col++ {
			dst[row*stride+col] = vals[row]
		}
	}
}

func predict4LD(dst []byte, stride int, above []byte) {
	if above == nil {
		return
	}
	a := above
	dst[0*stride+0] = avg3(a[0], a[1], a[2])
	dst[0*stride+1] = avg3(a[1], a[2], a[3])
	dst[1*stride+0] = dst[0*stride+1]
	dst[0*stride+2] = avg3(a[2], a[3], safeIdx(a, 4))
	dst[1*stride+1] = dst[0*stride+2]
	dst[2*stride+0] = dst[0*stride+2]
	dst[0*stride+3] = avg3(a[3], safeIdx(a, 4), safeIdx(a, 5))
	dst[1*stride+2] = dst[0*stride+3]
	dst[2*stride+1] = dst[0*stride+3]
	dst[3*stride+0] = dst[0*stride+3]
	v := avg3(safeIdx(a, 4), safeIdx(a, 5), safeIdx(a, 6))
	dst[1*stride+3] = v
	dst[2*stride+2] = v
	dst[3*stride+1] = v
	v = avg3(safeIdx(a, 5), safeIdx(a, 6), safeIdx(a, 7))
	dst[2*stride+3] = v
	dst[3*stride+2] = v
	dst[3*stride+3] = avg3(safeIdx(a, 6), safeIdx(a, 7), safeIdx(a, 7))
}

func predict4RD(dst []byte, stride int, above []byte, left []byte, al byte) {
	if above == nil || left == nil {
		return
	}
	// Build reference array: left[3],left[2],left[1],left[0],al,above[0..3]
	var ref [9]byte
	ref[0] = left[3]
	ref[1] = left[2]
	ref[2] = left[1]
	ref[3] = left[0]
	ref[4] = al
	copy(ref[5:], above[:4])
	for row := 0; row < 4; row++ {
		for col := 0; col < 4; col++ {
			i := 4 - row + col
			dst[row*stride+col] = avg3(ref[i-1], ref[i], ref[i+1])
		}
	}
}

func predict4VR(dst []byte, stride int, above []byte, left []byte, al byte) {
	if above == nil || left == nil {
		return
	}
	var ref [9]byte
	ref[0] = left[3]
	ref[1] = left[2]
	ref[2] = left[1]
	ref[3] = left[0]
	ref[4] = al
	copy(ref[5:], above[:4])
	for row := 0; row < 4; row++ {
		for col := 0; col < 4; col++ {
			i := 4 - (row-1)/2 + col
			if (row & 1) == 0 {
				i = 4 - row/2 + col
				dst[row*stride+col] = avg2(ref[i], ref[i+1])
			} else {
				dst[row*stride+col] = avg3(ref[i-1], ref[i], ref[i+1])
			}
		}
	}
}

func predict4VL(dst []byte, stride int, above []byte) {
	if above == nil {
		return
	}
	a := above
	for row := 0; row < 4; row++ {
		for col := 0; col < 4; col++ {
			i := col + row/2
			if (row & 1) == 0 {
				dst[row*stride+col] = avg2(safeIdx(a, i), safeIdx(a, i+1))
			} else {
				dst[row*stride+col] = avg3(safeIdx(a, i), safeIdx(a, i+1), safeIdx(a, i+2))
			}
		}
	}
}

func predict4HD(dst []byte, stride int, above []byte, left []byte, al byte) {
	if above == nil || left == nil {
		return
	}
	var ref [9]byte
	ref[0] = above[3]
	ref[1] = above[2]
	ref[2] = above[1]
	ref[3] = above[0]
	ref[4] = al
	ref[5] = left[0]
	ref[6] = left[1]
	ref[7] = left[2]
	ref[8] = left[3]
	for row := 0; row < 4; row++ {
		for col := 0; col < 4; col++ {
			i := 4 + row - col/2
			if (col & 1) == 0 {
				dst[row*stride+col] = avg2(ref[i], ref[i+1])
			} else {
				dst[row*stride+col] = avg3(ref[i-1], ref[i], ref[i+1])
			}
		}
	}
}

func predict4HU(dst []byte, stride int, left []byte) {
	if left == nil {
		return
	}
	l := left
	for row := 0; row < 4; row++ {
		for col := 0; col < 4; col++ {
			i := row + col/2
			if i >= 3 {
				dst[row*stride+col] = l[3]
			} else if (col & 1) == 0 {
				dst[row*stride+col] = avg2(l[i], l[i+1])
			} else {
				l2 := l[3]
				if i+2 < 4 {
					l2 = l[i+2]
				}
				dst[row*stride+col] = avg3(l[i], l[i+1], l2)
			}
		}
	}
}

func safeIdx(buf []byte, i int) byte {
	if i >= len(buf) {
		return buf[len(buf)-1]
	}
	return buf[i]
}

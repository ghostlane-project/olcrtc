package vp8

// VP8 loop filter per RFC 6386 section 15.
// Applies deblocking at macroblock and sub-block edges.

// loopFilter applies the in-loop deblocking filter to the reconstructed frame.
func loopFilter(frame *Frame, level, sharpness int, mbW, mbH int) {
	if level == 0 {
		return
	}
	iLimit, hevThresh := filterParams(level, sharpness)
	eLimit := iLimit + 2

	yStride := frame.yStride
	uvStride := frame.uvStride

	// Filter macroblock edges (strong filter) and sub-block edges (normal filter).
	for mbY := 0; mbY < mbH; mbY++ {
		for mbX := 0; mbX < mbW; mbX++ {
			// Vertical MB edge (left edge of MB).
			if mbX > 0 {
				filterMBEdgeV(frame.Y, yStride, mbX*16, mbY*16, 16, eLimit, iLimit, hevThresh)
				filterMBEdgeV(frame.U, uvStride, mbX*8, mbY*8, 8, eLimit, iLimit, hevThresh)
				filterMBEdgeV(frame.V, uvStride, mbX*8, mbY*8, 8, eLimit, iLimit, hevThresh)
			}
			// Horizontal MB edge (top edge of MB).
			if mbY > 0 {
				filterMBEdgeH(frame.Y, yStride, mbX*16, mbY*16, 16, eLimit, iLimit, hevThresh)
				filterMBEdgeH(frame.U, uvStride, mbX*8, mbY*8, 8, eLimit, iLimit, hevThresh)
				filterMBEdgeH(frame.V, uvStride, mbX*8, mbY*8, 8, eLimit, iLimit, hevThresh)
			}
			// Vertical sub-block edges within MB (Y only, at x=4,8,12).
			for bx := 1; bx < 4; bx++ {
				filterSubEdgeV(frame.Y, yStride, mbX*16+bx*4, mbY*16, 16, eLimit, iLimit, hevThresh)
			}
			// Horizontal sub-block edges within MB (Y only, at y=4,8,12).
			for by := 1; by < 4; by++ {
				filterSubEdgeH(frame.Y, yStride, mbX*16, mbY*16+by*4, 16, eLimit, iLimit, hevThresh)
			}
			// UV sub-block edges (at midpoint).
			filterSubEdgeV(frame.U, uvStride, mbX*8+4, mbY*8, 8, eLimit, iLimit, hevThresh)
			filterSubEdgeV(frame.V, uvStride, mbX*8+4, mbY*8, 8, eLimit, iLimit, hevThresh)
			filterSubEdgeH(frame.U, uvStride, mbX*8, mbY*8+4, 8, eLimit, iLimit, hevThresh)
			filterSubEdgeH(frame.V, uvStride, mbX*8, mbY*8+4, 8, eLimit, iLimit, hevThresh)
		}
	}
}

func filterParams(level, sharpness int) (iLimit, hevThresh int) {
	iLimit = level
	if sharpness > 0 {
		iLimit >>= 1
		if sharpness > 4 {
			iLimit >>= 1
		}
		if iLimit > 9-sharpness {
			iLimit = 9 - sharpness
		}
	}
	if iLimit < 1 {
		iLimit = 1
	}
	hevThresh = level >> 4
	return iLimit, hevThresh
}

// filterMBEdgeV applies the strong (MB-edge) vertical filter.
func filterMBEdgeV(plane []byte, stride, x, y, h, eLimit, iLimit, hevThresh int) {
	for row := 0; row < h; row++ {
		off := (y+row)*stride + x
		p3 := int(plane[off-4])
		p2 := int(plane[off-3])
		p1 := int(plane[off-2])
		p0 := int(plane[off-1])
		q0 := int(plane[off])
		q1 := int(plane[off+1])
		q2 := int(plane[off+2])
		q3 := int(plane[off+3])

		if !filterCommon(p3, p2, p1, p0, q0, q1, q2, q3, eLimit, iLimit) {
			continue
		}
		if useHev(p1, p0, q0, q1, hevThresh) {
			// High edge variance: simple filter.
			a := clampS(3*(q0-p0) + clampS(p1-q1))
			a1 := clampFilter(a+4) >> 3
			a2 := clampFilter(a+3) >> 3
			plane[off-1] = clampByte(p0 + a2)
			plane[off] = clampByte(q0 - a1)
		} else {
			// Wide filter for MB edges.
			a := clampS(3 * (q0 - p0))
			a1 := clampFilter(a+4) >> 3
			a2 := clampFilter(a+3) >> 3
			plane[off-1] = clampByte(p0 + a2)
			plane[off] = clampByte(q0 - a1)
			a = (a1 + 1) >> 1
			plane[off-2] = clampByte(p1 + a)
			plane[off+1] = clampByte(q1 - a)
		}
	}
}

// filterMBEdgeH applies the strong horizontal filter.
func filterMBEdgeH(plane []byte, stride, x, y, w, eLimit, iLimit, hevThresh int) {
	for col := 0; col < w; col++ {
		off := y*stride + x + col
		p3 := int(plane[off-4*stride])
		p2 := int(plane[off-3*stride])
		p1 := int(plane[off-2*stride])
		p0 := int(plane[off-stride])
		q0 := int(plane[off])
		q1 := int(plane[off+stride])
		q2 := int(plane[off+2*stride])
		q3 := int(plane[off+3*stride])

		if !filterCommon(p3, p2, p1, p0, q0, q1, q2, q3, eLimit, iLimit) {
			continue
		}
		if useHev(p1, p0, q0, q1, hevThresh) {
			a := clampS(3*(q0-p0) + clampS(p1-q1))
			a1 := clampFilter(a+4) >> 3
			a2 := clampFilter(a+3) >> 3
			plane[off-stride] = clampByte(p0 + a2)
			plane[off] = clampByte(q0 - a1)
		} else {
			a := clampS(3 * (q0 - p0))
			a1 := clampFilter(a+4) >> 3
			a2 := clampFilter(a+3) >> 3
			plane[off-stride] = clampByte(p0 + a2)
			plane[off] = clampByte(q0 - a1)
			a = (a1 + 1) >> 1
			plane[off-2*stride] = clampByte(p1 + a)
			plane[off+stride] = clampByte(q1 - a)
		}
	}
}

// filterSubEdgeV applies the normal (sub-block) vertical filter.
func filterSubEdgeV(plane []byte, stride, x, y, h, eLimit, iLimit, hevThresh int) {
	for row := 0; row < h; row++ {
		off := (y+row)*stride + x
		p2 := int(plane[off-3])
		p1 := int(plane[off-2])
		p0 := int(plane[off-1])
		q0 := int(plane[off])
		q1 := int(plane[off+1])
		q2 := int(plane[off+2])

		if !filterNormal(p2, p1, p0, q0, q1, q2, eLimit, iLimit) {
			continue
		}
		if useHev(p1, p0, q0, q1, hevThresh) {
			a := clampS(3*(q0-p0) + clampS(p1-q1))
			a1 := clampFilter(a+4) >> 3
			a2 := clampFilter(a+3) >> 3
			plane[off-1] = clampByte(p0 + a2)
			plane[off] = clampByte(q0 - a1)
		} else {
			a := clampS(3*(q0-p0) + clampS(p1-q1))
			a1 := clampFilter(a+4) >> 3
			a2 := clampFilter(a+3) >> 3
			plane[off-1] = clampByte(p0 + a2)
			plane[off] = clampByte(q0 - a1)
			a = (a1 + 1) >> 1
			plane[off-2] = clampByte(p1 + a)
			plane[off+1] = clampByte(q1 - a)
		}
	}
}

// filterSubEdgeH applies the normal horizontal filter.
func filterSubEdgeH(plane []byte, stride, x, y, w, eLimit, iLimit, hevThresh int) {
	for col := 0; col < w; col++ {
		off := y*stride + x + col
		p2 := int(plane[off-3*stride])
		p1 := int(plane[off-2*stride])
		p0 := int(plane[off-stride])
		q0 := int(plane[off])
		q1 := int(plane[off+stride])
		q2 := int(plane[off+2*stride])

		if !filterNormal(p2, p1, p0, q0, q1, q2, eLimit, iLimit) {
			continue
		}
		if useHev(p1, p0, q0, q1, hevThresh) {
			a := clampS(3*(q0-p0) + clampS(p1-q1))
			a1 := clampFilter(a+4) >> 3
			a2 := clampFilter(a+3) >> 3
			plane[off-stride] = clampByte(p0 + a2)
			plane[off] = clampByte(q0 - a1)
		} else {
			a := clampS(3*(q0-p0) + clampS(p1-q1))
			a1 := clampFilter(a+4) >> 3
			a2 := clampFilter(a+3) >> 3
			plane[off-stride] = clampByte(p0 + a2)
			plane[off] = clampByte(q0 - a1)
			a = (a1 + 1) >> 1
			plane[off-2*stride] = clampByte(p1 + a)
			plane[off+stride] = clampByte(q1 - a)
		}
	}
}

func filterCommon(p3, p2, p1, p0, q0, q1, q2, q3, eLimit, iLimit int) bool {
	if abs(p0-q0)*2+abs(p1-q1)/2 > eLimit {
		return false
	}
	return abs(p3-p2) <= iLimit && abs(p2-p1) <= iLimit &&
		abs(p1-p0) <= iLimit && abs(q1-q0) <= iLimit &&
		abs(q2-q1) <= iLimit && abs(q3-q2) <= iLimit
}

func filterNormal(p2, p1, p0, q0, q1, q2, eLimit, iLimit int) bool {
	if abs(p0-q0)*2+abs(p1-q1)/2 > eLimit {
		return false
	}
	return abs(p2-p1) <= iLimit && abs(p1-p0) <= iLimit &&
		abs(q1-q0) <= iLimit && abs(q2-q1) <= iLimit
}

func useHev(p1, p0, q0, q1, thresh int) bool {
	return abs(p1-p0) > thresh || abs(q1-q0) > thresh
}

func clampS(v int) int {
	if v < -128 {
		return -128
	}
	if v > 127 {
		return 127
	}
	return v
}

func clampFilter(v int) int {
	if v < -128 {
		return -128
	}
	if v > 127 {
		return 127
	}
	return v
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

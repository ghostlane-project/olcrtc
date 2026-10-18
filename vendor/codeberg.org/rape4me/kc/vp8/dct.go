package vp8

// VP8 uses the Walsh-Hadamard Transform (WHT) for the DC values of Y blocks
// and a simplified 4x4 DCT for all sub-blocks.

// fdct4x4 performs the forward 4x4 DCT used in VP8.
// Input: 16 residual values in raster order. Output: 16 coefficients.
// Matches libvpx's vp8_short_fdct4x4_c so output is bit-exact spec coeffs.
func fdct4x4(in [16]int16) [16]int16 {
	var tmp [16]int32
	// Horizontal pass.
	for i := 0; i < 4; i++ {
		a := (int32(in[i*4+0]) + int32(in[i*4+3])) * 8
		b := (int32(in[i*4+1]) + int32(in[i*4+2])) * 8
		c := (int32(in[i*4+1]) - int32(in[i*4+2])) * 8
		d := (int32(in[i*4+0]) - int32(in[i*4+3])) * 8
		tmp[i*4+0] = a + b
		tmp[i*4+2] = a - b
		tmp[i*4+1] = (c*2217 + d*5352 + 14500) >> 12
		tmp[i*4+3] = (d*2217 - c*5352 + 7500) >> 12
	}
	var out [16]int16
	// Vertical pass.
	for i := 0; i < 4; i++ {
		a := tmp[i] + tmp[12+i]
		b := tmp[4+i] + tmp[8+i]
		c := tmp[4+i] - tmp[8+i]
		d := tmp[i] - tmp[12+i]
		out[i] = int16((a + b + 7) >> 4)
		out[8+i] = int16((a - b + 7) >> 4)
		v := (c*2217 + d*5352 + 12000) >> 16
		if d != 0 {
			v++
		}
		out[4+i] = int16(v)
		out[12+i] = int16((d*2217 - c*5352 + 51000) >> 16)
	}
	return out
}

// idct4x4 performs the inverse 4x4 DCT used in VP8.
func idct4x4(in [16]int16) [16]int16 {
	// DC-only fast path.
	if in[1]|in[2]|in[3]|in[4]|in[5]|in[6]|in[7]|in[8]|in[9]|in[10]|in[11]|in[12]|in[13]|in[14]|in[15] == 0 {
		dc := int16((int32(in[0]) + 4) >> 3)
		return [16]int16{dc, dc, dc, dc, dc, dc, dc, dc, dc, dc, dc, dc, dc, dc, dc, dc}
	}
	var tmp [16]int32
	// Horizontal pass.
	for i := 0; i < 4; i++ {
		a := int32(in[i*4+0]) + int32(in[i*4+2])
		b := int32(in[i*4+0]) - int32(in[i*4+2])
		c := (int32(in[i*4+1]) * 35468 >> 16) - int32(in[i*4+3]) - (int32(in[i*4+3]) * 20091 >> 16)
		d := int32(in[i*4+1]) + (int32(in[i*4+1]) * 20091 >> 16) + (int32(in[i*4+3]) * 35468 >> 16)
		tmp[i*4+0] = a + d
		tmp[i*4+1] = b + c
		tmp[i*4+2] = b - c
		tmp[i*4+3] = a - d
	}
	var out [16]int16
	// Vertical pass.
	for i := 0; i < 4; i++ {
		a := tmp[i] + tmp[8+i]
		b := tmp[i] - tmp[8+i]
		c := (tmp[4+i] * 35468 >> 16) - tmp[12+i] - (tmp[12+i] * 20091 >> 16)
		d := tmp[4+i] + (tmp[4+i] * 20091 >> 16) + (tmp[12+i] * 35468 >> 16)
		out[i] = int16((a + d + 4) >> 3)
		out[4+i] = int16((b + c + 4) >> 3)
		out[8+i] = int16((b - c + 4) >> 3)
		out[12+i] = int16((a - d + 4) >> 3)
	}
	return out
}

// fwht4x4 performs the forward 4x4 Walsh-Hadamard Transform for Y2 DC block.
func fwht4x4(in [16]int16) [16]int16 {
	var out [16]int16
	for i := 0; i < 4; i++ {
		a := in[i*4+0] + in[i*4+2]
		d := in[i*4+1] + in[i*4+3]
		c := in[i*4+0] - in[i*4+2]
		b := in[i*4+1] - in[i*4+3]
		out[i*4+0] = a + d
		out[i*4+1] = c + b
		out[i*4+2] = a - d
		out[i*4+3] = c - b
	}
	var tmp [16]int16
	for i := 0; i < 4; i++ {
		a := out[i] + out[8+i]
		d := out[4+i] + out[12+i]
		c := out[i] - out[8+i]
		b := out[4+i] - out[12+i]
		tmp[i] = (a + d) >> 1
		tmp[4+i] = (c + b) >> 1
		tmp[8+i] = (a - d) >> 1
		tmp[12+i] = (c - b) >> 1
	}
	return tmp
}

// iwht4x4 performs the inverse 4x4 Walsh-Hadamard Transform.
func iwht4x4(in [16]int16) [16]int16 {
	var tmp [16]int16
	for i := 0; i < 4; i++ {
		a := in[i] + in[12+i]
		b := in[4+i] + in[8+i]
		c := in[4+i] - in[8+i]
		d := in[i] - in[12+i]
		tmp[i] = a + b
		tmp[4+i] = d + c
		tmp[8+i] = a - b
		tmp[12+i] = d - c
	}
	var out [16]int16
	for i := 0; i < 4; i++ {
		a := tmp[i*4+0] + tmp[i*4+3]
		b := tmp[i*4+1] + tmp[i*4+2]
		c := tmp[i*4+1] - tmp[i*4+2]
		d := tmp[i*4+0] - tmp[i*4+3]
		out[i*4+0] = (a + b + 3) >> 3
		out[i*4+1] = (d + c + 3) >> 3
		out[i*4+2] = (a - b + 3) >> 3
		out[i*4+3] = (d - c + 3) >> 3
	}
	return out
}

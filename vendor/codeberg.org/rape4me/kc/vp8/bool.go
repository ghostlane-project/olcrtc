package vp8

// VP8 boolean arithmetic encoder and decoder per RFC 6386 section 7.
// Implementation matches libvpx and ffmpeg byte-for-byte so encoded streams
// can be decoded by any compliant VP8 decoder.

// boolWriter mirrors libvpx's vp8_encode_bool / vp8_start_encode.
type boolWriter struct {
	buf      []byte
	bottom   uint32 // libvpx: lowvalue
	rng      uint32 // libvpx: range
	bitsLeft int    // libvpx: count (-24 means need 24 more bits before output)
}

func newBoolWriter() *boolWriter {
	return &boolWriter{rng: 255, bitsLeft: -24}
}

func newBoolWriterCap(initCap int) *boolWriter {
	return &boolWriter{buf: make([]byte, 0, initCap), rng: 255, bitsLeft: -24}
}

func (w *boolWriter) reset() {
	w.buf = w.buf[:0]
	w.rng = 255
	w.bottom = 0
	w.bitsLeft = -24
}

// writeBool encodes a boolean with prob/256 chance of being false.
// Direct port of libvpx vp8_encode_bool.
func (w *boolWriter) writeBool(prob uint8, val bool) {
	split := 1 + ((w.rng-1)*uint32(prob))>>8
	if val {
		w.bottom += split
		w.rng -= split
	} else {
		w.rng = split
	}
	// Normalization shift count.
	shift := 0
	for w.rng < 128 {
		w.rng <<= 1
		shift++
	}
	count := w.bitsLeft + shift
	if count >= 0 {
		offset := shift - count
		// Carry detection: checks bit (32-offset) of bottom (libvpx semantics).
		// For offset=0, check bit 0; for offset=1, check bit 31; etc.
		var bit32mOff uint32
		if offset == 0 {
			bit32mOff = 1
		} else {
			bit32mOff = 1 << uint(32-offset)
		}
		if w.bottom&bit32mOff != 0 {
			// Propagate carry into already-emitted bytes.
			for i := len(w.buf) - 1; i >= 0; i-- {
				w.buf[i]++
				if w.buf[i] != 0 {
					break
				}
			}
		}
		w.buf = append(w.buf, byte((w.bottom>>uint(24-offset))&0xff))
		w.bottom <<= uint(offset)
		w.bottom &= 0x00ffffff
		shift = count
		count -= 8
	}
	w.bottom <<= uint(shift)
	w.bitsLeft = count
}

func (w *boolWriter) writeLit(n int, val uint32) {
	for i := n - 1; i >= 0; i-- {
		w.writeBool(128, (val>>uint(i))&1 != 0)
	}
}

func (w *boolWriter) writeSignedLit(n int, val int32) {
	mag := uint32(val)
	sign := false
	if val < 0 {
		mag = uint32(-val)
		sign = true
	}
	w.writeLit(n, mag)
	w.writeBool(128, sign)
}

// flush emits enough bits to force out the final state. libvpx writes 32
// "false" bits with prob 128.
func (w *boolWriter) flush() {
	for i := 0; i < 32; i++ {
		w.writeBool(128, false)
	}
}

func (w *boolWriter) bytes() []byte {
	return w.buf
}

// boolReader mirrors libvpx's vp8 bool_decoder / ffmpeg vpx_rac.
// Maintains 32-bit code_word filled 16 bits at a time.
type boolReader struct {
	data     []byte
	pos      int
	codeWord uint32 // ffmpeg: code_word
	high     int    // ffmpeg: high (range)
	bits     int    // ffmpeg: bits (-16 means need 16 more bits to refill)
}

// nshifts is the count of leading 0 bits in `range`'s top byte after subtraction
// during normalization. Equivalent to ffmpeg's vp8_norm table.
var nshifts = [256]int{
	0, 7, 6, 6, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4,
	3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
	0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
}

func newBoolReader(data []byte) *boolReader {
	r := &boolReader{data: data, high: 255, bits: -16}
	if len(data) >= 3 {
		r.codeWord = uint32(data[0])<<16 | uint32(data[1])<<8 | uint32(data[2])
		r.pos = 3
	} else {
		// Pad short input.
		for i := 0; i < len(data); i++ {
			r.codeWord |= uint32(data[i]) << uint(16-8*i)
		}
		r.pos = len(data)
	}
	return r
}

// readBool decodes one bit with probability prob/256 of being false.
// Matches ffmpeg vpx_rac_get_prob_branchy.
func (r *boolReader) readBool(prob uint8) bool {
	sh := nshifts[r.high]
	r.high <<= sh
	cw := r.codeWord << uint(sh)
	bits := r.bits + sh
	if bits >= 0 && r.pos+1 < len(r.data) {
		cw |= (uint32(r.data[r.pos])<<8 | uint32(r.data[r.pos+1])) << uint(bits)
		r.pos += 2
		bits -= 16
	} else if bits >= 0 && r.pos < len(r.data) {
		cw |= uint32(r.data[r.pos]) << (8 + uint(bits))
		r.pos++
		bits -= 8
	}
	r.bits = bits
	low := 1 + ((r.high-1)*int(prob))>>8
	ls := uint32(low) << 16
	if cw >= ls {
		r.high -= low
		r.codeWord = cw - ls
		return true
	}
	r.high = low
	r.codeWord = cw
	return false
}

func (r *boolReader) readLit(n int) uint32 {
	var v uint32
	for i := 0; i < n; i++ {
		if r.readBool(128) {
			v = v<<1 | 1
		} else {
			v <<= 1
		}
	}
	return v
}

func (r *boolReader) readSignedLit(n int) int32 {
	mag := r.readLit(n)
	if r.readBool(128) {
		return -int32(mag)
	}
	return int32(mag)
}

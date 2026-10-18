package vp8

// Test helpers for bool encoder validation against ffmpeg-style decoder.

type BoolWriterTest struct {
	w *boolWriter
}

func NewBoolWriterTest() *BoolWriterTest {
	return &BoolWriterTest{w: newBoolWriter()}
}

func (b *BoolWriterTest) WriteBool(prob uint8, val bool) {
	b.w.writeBool(prob, val)
}

func (b *BoolWriterTest) Flush() {
	b.w.flush()
}

func (b *BoolWriterTest) Bytes() []byte {
	return b.w.bytes()
}

#include "textflag.h"

// func dotAVX2(a, b []float32) float64
// Y0 to Y3 hold the 16 partial sums: Y0 elements 16i+0..3 (and a last
// block of 4), Y1 16i+4..7, Y2 16i+8..11, Y3 16i+12..15.
TEXT ·dotAVX2(SB), NOSPLIT, $0-56
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI
	VXORPD Y0, Y0, Y0
	VXORPD Y1, Y1, Y1
	VXORPD Y2, Y2, Y2
	VXORPD Y3, Y3, Y3
	XORQ AX, AX

blocks16:
	LEAQ 16(AX), DX
	CMPQ DX, CX
	JGT  blocks4
	VCVTPS2PD (SI)(AX*4), Y4
	VCVTPS2PD (DI)(AX*4), Y5
	VFMADD231PD Y5, Y4, Y0
	VCVTPS2PD 16(SI)(AX*4), Y6
	VCVTPS2PD 16(DI)(AX*4), Y7
	VFMADD231PD Y7, Y6, Y1
	VCVTPS2PD 32(SI)(AX*4), Y8
	VCVTPS2PD 32(DI)(AX*4), Y9
	VFMADD231PD Y9, Y8, Y2
	VCVTPS2PD 48(SI)(AX*4), Y10
	VCVTPS2PD 48(DI)(AX*4), Y11
	VFMADD231PD Y11, Y10, Y3
	MOVQ DX, AX
	JMP  blocks16

blocks4:
	LEAQ 4(AX), DX
	CMPQ DX, CX
	JGT  lanes
	VCVTPS2PD (SI)(AX*4), Y4
	VCVTPS2PD (DI)(AX*4), Y5
	VFMADD231PD Y5, Y4, Y0
	MOVQ DX, AX
	JMP  blocks4

lanes:
	VADDPD Y1, Y0, Y0         // acc0 + acc1
	VADDPD Y3, Y2, Y2         // acc2 + acc3
	VADDPD Y2, Y0, Y0         // v
	VEXTRACTF128 $1, Y0, X1   // v2, v3
	VADDPD X1, X0, X0         // v0 + v2, v1 + v3
	VHADDPD X0, X0, X0        // (v0 + v2) + (v1 + v3)

singles:
	CMPQ AX, CX
	JGE  done
	VCVTSS2SD (SI)(AX*4), X2, X2
	VCVTSS2SD (DI)(AX*4), X3, X3
	VFMADD231SD X3, X2, X0
	INCQ AX
	JMP  singles

done:
	VZEROUPPER
	MOVSD X0, ret+48(FP)
	RET

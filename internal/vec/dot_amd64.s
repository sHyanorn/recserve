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

// func dot32AVX2(a, b []float32) float32
// Y0 to Y3 hold the 32 partial sums: Y0 elements 32i+0..7 (and the last
// blocks of 8), Y1 32i+8..15, Y2 32i+16..23, Y3 32i+24..31. Products and
// sums are rounded separately, as in dot32Go: no FMA.
TEXT ·dot32AVX2(SB), NOSPLIT, $0-52
	MOVQ a_base+0(FP), SI
	MOVQ a_len+8(FP), CX
	MOVQ b_base+24(FP), DI
	VXORPS Y0, Y0, Y0
	VXORPS Y1, Y1, Y1
	VXORPS Y2, Y2, Y2
	VXORPS Y3, Y3, Y3
	XORQ AX, AX

blocks32:
	LEAQ 32(AX), DX
	CMPQ DX, CX
	JGT  blocks8
	VMOVUPS (SI)(AX*4), Y4
	VMULPS  (DI)(AX*4), Y4, Y4
	VADDPS  Y4, Y0, Y0
	VMOVUPS 32(SI)(AX*4), Y5
	VMULPS  32(DI)(AX*4), Y5, Y5
	VADDPS  Y5, Y1, Y1
	VMOVUPS 64(SI)(AX*4), Y6
	VMULPS  64(DI)(AX*4), Y6, Y6
	VADDPS  Y6, Y2, Y2
	VMOVUPS 96(SI)(AX*4), Y7
	VMULPS  96(DI)(AX*4), Y7, Y7
	VADDPS  Y7, Y3, Y3
	MOVQ DX, AX
	JMP  blocks32

blocks8:
	LEAQ 8(AX), DX
	CMPQ DX, CX
	JGT  lanes32
	VMOVUPS (SI)(AX*4), Y4
	VMULPS  (DI)(AX*4), Y4, Y4
	VADDPS  Y4, Y0, Y0
	MOVQ DX, AX
	JMP  blocks8

lanes32:
	VADDPS Y1, Y0, Y0         // acc0 + acc1
	VADDPS Y3, Y2, Y2         // acc2 + acc3
	VADDPS Y2, Y0, Y0         // v
	VEXTRACTF128 $1, Y0, X1   // v4..v7
	VADDPS X1, X0, X0         // t_l = v_l + v_l+4
	VMOVHLPS X0, X1, X1       // t2, t3
	VADDPS X1, X0, X0         // t0 + t2, t1 + t3
	VMOVSHDUP X0, X1          // t1 + t3
	VADDSS X1, X0, X0         // (t0 + t2) + (t1 + t3)

singles32:
	CMPQ AX, CX
	JGE  done32
	VMOVSS (SI)(AX*4), X2
	VMULSS (DI)(AX*4), X2, X2
	VADDSS X2, X0, X0
	INCQ AX
	JMP  singles32

done32:
	VZEROUPPER
	MOVSS X0, ret+48(FP)
	RET

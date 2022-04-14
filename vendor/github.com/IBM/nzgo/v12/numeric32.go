package nzgo

const NDIGIT_INT64 bool = false
const MAX_NUMERIC_DIGIT_COUNT = 4
const NUMERIC_MAX_PRECISION = 38
const HI32_MASK uint64 = 0xffffffff00000000
const USE_MUL_DOUBLE bool = false

type TNumericDigit uint32

type TNumericData struct {
	digit [MAX_NUMERIC_DIGIT_COUNT]TNumericDigit /* digit[0] is hi order */
}

type NumericVar struct {
	data       TNumericData /* value */
	scale      int          /* scale of 'data' */
	rscale     int          /* logical result scale */
	rprecision int          /* logical result precision */
}

func base() uint64 {
	return (uint64(1) << 32)
}
func highPart(val uint64) uint64 {
	return (val >> 32)
}

func lowPart(val uint64) uint64 {
	return (val & ((uint64(1) << 32) - 1))
}

func encodeNum(words []int64, low int64, hi int64) {
	words[0] = int64(lowPart(uint64(low)))
	words[1] = int64(highPart(uint64(low)))
	words[2] = int64(lowPart(uint64(hi)))
	words[3] = int64(highPart(uint64(hi)))
}

/* Pack an array of 4 words into a two-word integer.
   WORDS points to the array of words.
   The integer is stored into *LOW and *HI as two `int64' pieces.
*/
func decodeNum(words []int64, low *int64, hi *int64) {
	*low = words[0] | words[1]*(int64(1)<<32)
	*hi = words[2] | words[3]*(int64(1)<<32)
}

//Check if numeric variable value is negative
func isNumeric_Data_Negative(numdataP TNumericData) bool {
	return (numdataP.digit[0] & 0x80000000) != 0
}

func copy_128(destP *TNumericData, srcP *TNumericData) {
	destP.digit[0] = srcP.digit[0]
	destP.digit[1] = srcP.digit[1]
	destP.digit[2] = srcP.digit[2]
	destP.digit[3] = srcP.digit[3]
}

func negate_128(arg *TNumericData) bool {
	//First complement the value (1's complement)
	for i := 0; i < MAX_NUMERIC_DIGIT_COUNT; i++ {
		arg.digit[i] = ^(arg.digit[i])
	}
	//Then increment it to form 2's complement (negative)
	return inc_128(arg)
}

//for 2's complement
func inc_128(arg *TNumericData) bool {
	i := MAX_NUMERIC_DIGIT_COUNT
	var work int64
	var carry bool = true
	bInputNegative := isNumeric_Data_Negative(*arg)

	for (i != 0) && carry {
		i -= 1
		work = (int64(arg.digit[i])) + 1
		carry = (uint64(work) & HI32_MASK) != 0
		arg.digit[i] = (TNumericDigit)(work & 0xffffffff)
	}

	if !bInputNegative {
		return isNumeric_Data_Negative(*arg)
	} else {
		return false
	}
}

func div10_128(numeratorP *TNumericData, quotientP *TNumericData) int {
	var remainder int = 0
	var work int64

	for i := 0; i < MAX_NUMERIC_DIGIT_COUNT; i++ {
		work = int64(uint64(numeratorP.digit[i]) + uint64(remainder)<<32)
		if work != 0 {
			quotientP.digit[i] = (TNumericDigit)(work / 10)
			remainder = int(work % 10)
		} else {
			quotientP.digit[i] = 0
			remainder = 0
		}
	}
	return (remainder)
}

//Get Numeric variable value represented in string format
func get_str_from_var(nvar *NumericVar, dscale int) string {
	var workData TNumericData
	var unbiasedDigits [NUMERIC_MAX_PRECISION]int
	var iplaces int
	var tmp int
	var work [NUMERIC_MAX_PRECISION + 1]byte

	var res [NUMERIC_MAX_PRECISION + 4]byte //Sign byte, . byte, nul terminating string
	var bLeadingZeroes bool = true
	var pos int = 0
	bNegative := isNumeric_Data_Negative(nvar.data)

	if round_var(nvar, dscale) {
		return ""
	}

	copy_128(&workData, &nvar.data)

	if bNegative {
		if negate_128(&workData) {
			return ""
		}
	}

	for tmp = 0; tmp < NUMERIC_MAX_PRECISION; tmp++ {
		unbiasedDigits[NUMERIC_MAX_PRECISION-tmp-1] = div10_128(&workData, &workData)
	}

	for tmp = 0; tmp < NUMERIC_MAX_PRECISION; tmp++ {
		// suppress leading zeros, but force output of a digit before implied decimal point
		if (tmp < NUMERIC_MAX_PRECISION-dscale-1) && bLeadingZeroes && (unbiasedDigits[tmp] == 0) {
			continue
		}
		bLeadingZeroes = false
		work[pos] = byte(unbiasedDigits[tmp] + '0')
		pos += 1
	}
	work[pos] = 0 // terminate sork string

	tmp = pos // strlen of work data

	pos = 0 // to start updating result buffer
	if bNegative {
		res[pos] = '-'
		pos++
	}

	if dscale != 0 {
		iplaces = tmp - dscale //value before decimal
		for i := 0; i < iplaces; i++ {
			res[pos] = work[i]
			pos++
		}
		res[pos] = '.' //decimal point
		pos++
		for i := 0; i <= dscale; i++ { // 1 more size to copy \0
			res[pos] = work[i+iplaces]
			pos++
		}
	} else {
		for i := 0; i <= tmp; i++ {
			res[pos] = work[i]
			pos++
		}
	}

	dstSpace := string(res[:pos])
	return dstSpace
}

func CTable_i_fieldPrecision(tupdesc DbosTupleDesc, coldex int) int {
	return (((tupdesc.field_size[coldex]) >> 8) & 0x7F)
}

func CTable_i_fieldScale(tupdesc DbosTupleDesc, coldex int) int {
	return ((tupdesc.field_size[coldex]) & 0x00FF)
}

func CTable_i_fieldNumericDigit32Count(tupdesc DbosTupleDesc, coldex int) int {
	var sizeTNumericDigit int
	sizeTNumericDigit = 4
	return (tupdesc.field_trueSize[coldex] / sizeTNumericDigit) //sizeof(TNumericDigit)
}

func GOLANG_numeric_load_var(varP *NumericVar, dataP []TNumericDigit, precision int, scale int, digitCount int) {
	var leadDigit TNumericDigit
	//extend sign
	sign := dataP[0] & 0x80000000
	if sign != 0 {
		leadDigit = 0xffffffff
	} else {
		leadDigit = 0
	}

	var i int
	for i = 0; i < MAX_NUMERIC_DIGIT_COUNT-digitCount; i++ {
		varP.data.digit[i] = leadDigit
	}
	j := 0
	for i < MAX_NUMERIC_DIGIT_COUNT {
		varP.data.digit[i] = dataP[j]
		j++
		i++
	}
	varP.scale = scale
	varP.rscale = scale
	varP.rprecision = precision
}

/* ----------
 * round_var() -
 *      Rounds a numeric var to a target scale.
 *  overflow will return true.
 * ----------
 */
var const_data_ten TNumericData

func round_var(nvar *NumericVar, scale int) bool {
	for i := 0; i < MAX_NUMERIC_DIGIT_COUNT; i++ {
		const_data_ten.digit[i] = 0
	}
	const_data_ten.digit[MAX_NUMERIC_DIGIT_COUNT-1] = 10

	var aDD int = scale - nvar.scale // additional decimal digits
	var positive bool = !isNumeric_Data_Negative(nvar.data)
	var workData, temp TNumericData
	var round bool

	if nvar.scale == scale {
		return false
	}

	copy_128(&workData, &nvar.data)
	if !positive {
		if negate_128(&workData) {
			return true
		}
	}

	if aDD < 0 {
		if (aDD != -1) && div_128(&workData, power_of_10(-aDD-1), &workData) {
			return true
		}

		round = (div10_128(&workData, &temp) > 4) //for rounding ro next number
		if div_128(&workData, &const_data_ten, &workData) {
			return true
		}
		if round {
			if inc_128(&workData) {
				return true
			}
		}
	} else if aDD > 0 {
		if mul_128(&workData, power_of_10(aDD), &workData) {
			return true
		}
	}

	nvar.scale = scale
	nvar.rscale = scale // !FIX-jpb   rounding should change result scale, right?
	copy_128(&nvar.data, &workData)
	if !positive {
		if negate_128(&nvar.data) {
			return true
		}
	}
	return false
}

/* Multiply two doubleword integers with doubleword result.
   Return nonzero if the operation overflows, assuming it's signed.
   Each argument is given as two `int64' pieces.
   One argument is L1 and H1; the other, L2 and H2.
   The value is stored as two `int64' pieces in *LV and *HV.
*/
func mul_double(l1 int64, h1 int64, l2 int64, h2 int64, lv *int64, hv *int64) bool {
	var arg1 [4]int64
	var arg2 [4]int64
	var prod = [4 * 2]int64{0, 0, 0, 0, 0, 0, 0, 0}
	var carry uint64
	var i, j, k int
	var toplow, tophigh, neglow, neghigh int64

	encodeNum(arg1[:4], l1, h1)
	encodeNum(arg2[:4], l2, h2)

	for i = 0; i < 4; i++ {
		carry = 0
		for j = 0; j < 4; j++ {
			k = i + j
			/* This product is <= 0xFFFE0001, the sum <= 0xFFFF0000.  */
			carry += uint64(arg1[i] * arg2[j])
			/* Since prod[p] < 0xFFFF, this sum <= 0xFFFFFFFF.  */
			carry += uint64(prod[k])
			prod[k] = int64(lowPart(carry))
			carry = highPart(carry)
		}
		prod[i+4] = int64(carry)
	}

	decodeNum(prod[:4], lv, hv) /* This ignores prod[4] through prod[4*2-1] */

	/* Check for overflow by calculating the top half of the answer in full;
	   it should agree with the low half's sign bit.  */
	decodeNum(prod[4:], &toplow, &tophigh)
	if h1 < 0 {
		neg_double(l2, h2, &neglow, &neghigh)
		add_double(neglow, neghigh, toplow, tophigh, &toplow, &tophigh)
	}
	if h2 < 0 {
		neg_double(l1, h1, &neglow, &neghigh)
		add_double(neglow, neghigh, toplow, tophigh, &toplow, &tophigh)
	}
	if *hv < 0 {
		return ((^(toplow & tophigh)) != 0)
	} else {
		return ((toplow | tophigh) != 0)
	}

}

/* Negate a doubleword integer with doubleword result.
   Return nonzero if the operation overflows, assuming it's signed.
   The argument is given as two `int64' pieces in L1 and H1.
   The value is stored as two `int64' pieces in *LV and *HV.
*/
func neg_double(l1 int64, h1 int64, lv *int64, hv *int64) int {
	if l1 == 0 {
		*lv = 0
		*hv = -h1
		if (*hv & h1) < 0 {
			return 1
		} else {
			return 0
		}
	} else {
		*lv = -l1
		*hv = ^h1
		return 0
	}
}

func overflow_sum_sign(a int64, b int64, sum int64) int {
	if (^((a) ^ (b)) & ((a) ^ (sum))) < 0 {
		return 1
	} else {
		return 0
	}
}

/* Add two doubleword integers with doubleword result.
   Each argument is given as two `int64' pieces.
   One argument is L1 and H1; the other, L2 and H2.
   The value is stored as two `int64' pieces in *LV and *HV.
*/
func add_double(l1 int64, h1 int64, l2 int64, h2 int64, lv *int64, hv *int64) int {
	var l, h int64

	l = l1 + l2
	var badd int64
	if uint64(l) < uint64(l1) {
		badd = 1
	} else {
		badd = 0
	}
	h = h1 + h2 + badd

	*lv = l
	*hv = h
	return overflow_sum_sign(h1, h2, h)
}

func mul_128(v1 *TNumericData, v2 *TNumericData, vRes *TNumericData) bool {
	// We treat the arguments as having 8 16-bit digits and do long multiplications
	// as in the days of the 3 Rs
	//
	// Here's an example with 3 digit numbers written ABC and DEF.  C and F are in the
	// units' (i.e. "base to the zeroth power) position, B and E in the "base" position,
	// and A and D in the "base squared" position.
	//
	// The units position of the product will be the low digit of C*F.
	// The "base" position of the product will be the low digit of (B*F+C*E) plus
	// the carry digit from the first step (this is the high digit of the product C*F.
	// The "base squared" position of the product will be the low digit of (A*F+B*E+C*D)
	// plus carry.  The "base cubed" position of the product will be the low digit of
	// (A*E+B*D) plus carry. The "base to the fourth" position of the product will be
	// the low digit of A*D plus carry.  And the "base to the fifth" position of the
	// product will be the carry.
	//
	// We load the 4 32-bit digit value of "v1" into the 8 16-bit digit value "a",
	// load "v1" into "b", compute the 32-bit sums of products, as in the example
	// above, and store them into the 15 32-bit digit "work" value.  Finally, we
	// step through the "work" value entries, adding any carry, and assigning the low
	// 16-bit digit of each entry to the corresponding 16-bit digit of the 16 16-bit
	// digit result "c"
	//
	// !FIX-jpb this should be optimized

	if USE_MUL_DOUBLE {
		var l1, h1, l2, h2, lv, hv int64
		var bRetVal bool

		h1 = (int64(v1.digit[0]) << 32) + int64(v1.digit[1])
		l1 = (int64(v1.digit[2]) << 32) + int64(v1.digit[3])
		h2 = (int64(v2.digit[0]) << 32) + int64(v2.digit[1])
		l2 = (int64(v2.digit[2]) << 32) + int64(v2.digit[3])
		bRetVal = mul_double(l1, h1, l2, h2, &lv, &hv)
		vRes.digit[0] = (TNumericDigit)((uint64(hv) & HI32_MASK) >> 32)
		vRes.digit[1] = (TNumericDigit)(hv & 0xffffffff)
		vRes.digit[2] = (TNumericDigit)((uint64(lv) & HI32_MASK) >> 32)
		vRes.digit[3] = (TNumericDigit)(lv & 0xffffffff)
		return bRetVal
	} else {

		var v1abs, v2abs TNumericData
		var a [8]uint16
		var b [8]uint16
		var c [16]uint16
		var w [15]uint32
		var carry uint16 = 0
		var i int
		var bRetVal bool
		var bResNegative bool
		var val uint64

		if (v1.digit[0] | v1.digit[1] | v1.digit[2] |
			v2.digit[0] | v2.digit[1] | v2.digit[2]) == 0 {
			val = uint64(v1.digit[3]) * uint64(v2.digit[3])
			vRes.digit[3] = (TNumericDigit)(val & 0xFFFFFFFF)
			vRes.digit[2] = (TNumericDigit)(val >> 32)
			vRes.digit[1] = 0
			vRes.digit[0] = 0
			return (false)
		}

		bResNegative = (isNumeric_Data_Negative(*v1) != isNumeric_Data_Negative(*v2))
		copy_128(&v1abs, v1)
		if isNumeric_Data_Negative(v1abs) {
			if negate_128(&v1abs) {
				return true
			}
		}
		copy_128(&v2abs, v2)
		if isNumeric_Data_Negative(v2abs) {
			if negate_128(&v2abs) {
				return true
			}
		}

		load_8_digit(a[:], &v1abs)
		load_8_digit(b[:], &v2abs)

		w[0] = uint32(a[0]) * uint32(b[0])

		w[1] = uint32(a[1])*uint32(b[0]) + uint32(a[0])*uint32(b[1])

		w[2] = uint32(a[2])*uint32(b[0]) + uint32(a[1])*uint32(b[1]) + uint32(a[0])*uint32(b[2])

		w[3] = uint32(a[3])*uint32(b[0]) + uint32(a[2])*uint32(b[1]) + uint32(a[1])*uint32(b[2]) + uint32(a[0])*uint32(b[3])

		w[4] = uint32(a[4])*uint32(b[0]) + uint32(a[3])*uint32(b[1]) + uint32(a[2])*uint32(b[2]) + uint32(a[1])*uint32(b[3]) +
			uint32(a[0])*uint32(b[4])

		w[5] = uint32(a[5])*uint32(b[0]) + uint32(a[4])*uint32(b[1]) + uint32(a[3])*uint32(b[2]) + uint32(a[2])*uint32(b[3]) +
			uint32(a[1])*uint32(b[4]) + uint32(a[0])*uint32(b[5])

		w[6] = uint32(a[6])*uint32(b[0]) + uint32(a[5])*uint32(b[1]) + uint32(a[4])*uint32(b[2]) + uint32(a[3])*uint32(b[3]) +
			uint32(a[2])*uint32(b[4]) + uint32(a[1])*uint32(b[5]) + uint32(a[0])*uint32(b[6])

		w[7] = uint32(a[7])*uint32(b[0]) + uint32(a[6])*uint32(b[1]) + uint32(a[5])*uint32(b[2]) + uint32(a[4])*uint32(b[3]) +
			uint32(a[3])*uint32(b[4]) + uint32(a[2])*uint32(b[5]) + uint32(a[1])*uint32(b[6]) + uint32(a[0])*uint32(b[7])

		w[8] = uint32(a[7])*uint32(b[1]) + uint32(a[6])*uint32(b[2]) + uint32(a[5])*uint32(b[3]) + uint32(a[4])*uint32(b[4]) +
			uint32(a[3])*uint32(b[5]) + uint32(a[2])*uint32(b[6]) + uint32(a[1])*uint32(b[7])

		w[9] = uint32(a[7])*uint32(b[2]) + uint32(a[6])*uint32(b[3]) + uint32(a[5])*uint32(b[4]) + uint32(a[4])*uint32(b[5]) +
			uint32(a[3])*uint32(b[6]) + uint32(a[2])*uint32(b[7])

		w[10] = uint32(a[7])*uint32(b[3]) + uint32(a[6])*uint32(b[4]) + uint32(a[5])*uint32(b[5]) + uint32(a[4])*uint32(b[6]) +
			uint32(a[3])*uint32(b[7])

		w[11] = uint32(a[7])*uint32(b[4]) + uint32(a[6]*b[5]) + uint32(a[5])*uint32(b[6]) + uint32(a[4])*uint32(b[7])

		w[12] = uint32(a[7])*uint32(b[5]) + uint32(a[6])*uint32(b[6]) + uint32(a[5])*uint32(b[7])

		w[13] = uint32(a[7])*uint32(b[6]) + uint32(a[6])*uint32(b[7])

		w[14] = uint32(a[7]) * uint32(b[7])

		for i = 15; i > 0; i-- {
			w[i-1] += uint32(carry)
			c[i] = (uint16)(w[i-1] & 0xffff)
			carry = (uint16)(w[i-1] >> 16)
		}
		c[0] = carry // hi order digit is final carry
		bRetVal = store_8_digit_from_16(c[:], vRes)
		if bResNegative {
			if negate_128(vRes) {
				return true
			}
		}
		return (bRetVal)
	}

}

func load_8_digit(dest []uint16, src *TNumericData) {
	var i int

	for i = 0; i < MAX_NUMERIC_DIGIT_COUNT; i++ {
		dest[2*i] = uint16(uint32(src.digit[i] >> 16))
		dest[2*i+1] = uint16(uint32(src.digit[i] & 0xffffffff))
	}
}

// tests the hi order 8 digits of src[] for overflow and stores the low order 8 in dest
func store_8_digit_from_16(src []uint16, dest *TNumericData) bool {
	var i, j int

	for i = 0; i < 2*MAX_NUMERIC_DIGIT_COUNT; i++ {
		if src[i] != 0 {
			return true // overflow
		}
	}
	for j = 0; j < MAX_NUMERIC_DIGIT_COUNT; i += 2 {
		dest.digit[j] = TNumericDigit((uint32(src[i]) << 16) + uint32(src[i+1]))
		j += 1
	}
	return false
}

// mul10_and_add multiplies non-negative TNumericData in place by 10 and adds an int
func mul10_and_add(data *TNumericData, adder int) bool {
	i := MAX_NUMERIC_DIGIT_COUNT - 1
	var work uint64
	var carry uint32 = uint32(adder)

	for ; i >= 0; i -= 1 {
		work = (uint64(data.digit[i]))*uint64(10) + uint64(carry)
		data.digit[i] = TNumericDigit(work & 0xffffffff)
		carry = (uint32)(work >> 32)
	}

	return (carry != 0) // true=> overflow
}

func power_of_10(exponent int) *TNumericData {
	var powersOfTen [NUMERIC_MAX_PRECISION]TNumericData
	var needsInit bool = true
	var next TNumericData
	var i int

	if needsInit {
		next.digit[0] = 0
		next.digit[1] = 0
		next.digit[2] = 0
		next.digit[3] = 1

		for i = 0; i < NUMERIC_MAX_PRECISION; i++ {
			powersOfTen[i].digit[0] = next.digit[0]
			powersOfTen[i].digit[1] = next.digit[1]
			powersOfTen[i].digit[2] = next.digit[2]
			powersOfTen[i].digit[3] = next.digit[3]
			if mul10_and_add(&next, 0) { // use convenient helper routine in this one-time initing
				//                assert(false); // shouldn't happen if our loop limit correct
			}
		}
		needsInit = false
	}
	if exponent < NUMERIC_MAX_PRECISION {
		return &powersOfTen[exponent]
	} else if exponent == NUMERIC_MAX_PRECISION {
		return &powersOfTen[0] // NUMERIC_MAX_PRECISIONth needed for get_digit_count, but entry not used
	} else {
		return nil // This will never arise as its made sure scale will limit to MAX_NUMERIC_DIGIT_COUNT
	}

}

func div_128(numeratorP *TNumericData, denominatorP *TNumericData, resultP *TNumericData) bool {
	var hidenom, lodenom, hinum, lonum, hiquotient, loquotient, hiremainder, loremainder int64
	var num, den TNumericData
	var bResNegative bool = (isNumeric_Data_Negative(*numeratorP) != isNumeric_Data_Negative(*denominatorP))

	copy_128(&num, numeratorP)
	if isNumeric_Data_Negative(num) {
		if negate_128(&num) {
			return true
		}
	}
	copy_128(&den, denominatorP)
	if isNumeric_Data_Negative(den) {
		if negate_128(&den) {
			return true
		}
	}

	hinum = (int64(num.digit[0]) << 32) + int64(num.digit[1])
	lonum = (int64(num.digit[2]) << 32) + int64(num.digit[3])
	hidenom = (int64(den.digit[0]) << 32) + int64(den.digit[1])
	lodenom = (int64(den.digit[2]) << 32) + int64(den.digit[3])

	if div_and_round_double(1, lonum, hinum, lodenom, hidenom, &loquotient,
		&hiquotient, &loremainder, &hiremainder) != 0 {
		return true
	}

	resultP.digit[0] = (TNumericDigit)((uint64(hiquotient) & HI32_MASK) >> 32)
	resultP.digit[1] = (TNumericDigit)(hiquotient & 0xffffffff)
	resultP.digit[2] = (TNumericDigit)((uint64(loquotient) & HI32_MASK) >> 32)
	resultP.digit[3] = (TNumericDigit)(loquotient & 0xffffffff)
	if bResNegative {
		if negate_128(resultP) {
			return true
		}
	}
	return false

}

/* Divide doubleword integer LNUM, HNUM by doubleword integer LDEN, HDEN
   for a quotient (stored in *LQUO, *HQUO) and remainder (in *LREM, *HREM).
   CODE is a tree code for a kind of division, one of
   TRUNC_DIV_EXPR, FLOOR_DIV_EXPR, CEIL_DIV_EXPR, ROUND_DIV_EXPR
   or EXACT_DIV_EXPR
   It controls how the quotient is rounded to a integer.
   Return nonzero if the operation overflows.
   UNS nonzero says do unsigned division.
*/
func div_and_round_double(uns int, lnum_orig int64, hnum_orig int64,
	lden_orig int64, hden_orig int64,
	lquo *int64, hquo *int64,
	lrem *int64, hrem *int64) int {
	var quo_neg int = 0
	var num = [4 + 1]int64{0, 0, 0, 0, 0} /* extra element for scaling.  */
	var den = [4]int64{0, 0, 0, 0}
	var quo = [4]int64{0, 0, 0, 0}
	var i, j int //register int i, j;
	var work uint64
	var carry uint64 = 0 //register UNSIGNEDINT64 carry = 0;
	var lnum int64 = lnum_orig
	var hnum int64 = hnum_orig
	var lden int64 = lden_orig
	var hden int64 = hden_orig
	var overflow int = 0

	/* calculate quotient sign and convert operands to unsigned.  */
	if uns != 0 {
		if hnum < 0 {
			quo_neg = ^quo_neg //~ quo_neg;
			/* (minimum integer) / (-1) is the only overflow case.  */
			if (neg_double(lnum, hnum, &lnum, &hnum) != 0) && ((lden & hden) == -1) {
				overflow = 1
			}
		}
		if hden < 0 {
			quo_neg = ^quo_neg //~ quo_neg;
			neg_double(lden, hden, &lden, &hden)
		}
	}

	if hnum == 0 && hden == 0 { /* single precision */
		*hquo = 0
		*hrem = 0
		/* This unsigned division rounds toward zero.  */
		*lquo = int64(uint64(lnum) / uint64(lden))
		goto finish_up
	}

	if hnum == 0 { /* trivial case: dividend < divisor */
		/* hden != 0 already checked.  */
		*hquo = 0
		*lquo = 0
		*hrem = hnum
		*lrem = lnum
		goto finish_up
	}

	encodeNum(num[:4], lnum, hnum)
	encodeNum(den[:4], lden, hden)

	/* Special code for when the divisor < BASE.  */
	if hden == 0 && uint64(lden) < base() {
		/* hnum != 0 already checked.  */
		for i = 4 - 1; i >= 0; i-- {
			work = uint64(num[i]) + carry*base()
			quo[i] = int64(uint64(work) / uint64(lden))
			carry = work % uint64(lden)
		}
	} else {
		/* Full double precision division,
		   with thanks to Don Knuth's "Seminumerical Algorithms".  */
		var num_hi_sig, den_hi_sig int
		var quo_est, scale uint64

		/* Find the highest non-zero divisor digit.  */
		for i = 4 - 1; ; i-- {
			if den[i] != 0 {
				den_hi_sig = i
				break
			}
		}

		/* Insure that the first digit of the divisor is at least BASE/2.
		   This is required by the quotient digit estimation algorithm.  */

		scale = base() / uint64(den[den_hi_sig]+1)
		if scale > 1 { /* scale divisor and dividend */
			carry = 0
			for i = 0; i <= 4-1; i++ {
				work = (uint64(num[i]) * scale) + carry
				num[i] = int64(lowPart(work))
				carry = highPart(work)
			}
			num[4] = int64(carry)
			carry = 0
			for i = 0; i <= 4-1; i++ {
				work = (uint64(den[i]) * scale) + carry
				den[i] = int64(lowPart(work))
				carry = highPart(work)
				if den[i] != 0 {
					den_hi_sig = i
				}
			}
		}

		num_hi_sig = 4

		/* Main loop */
		for i = num_hi_sig - den_hi_sig - 1; i >= 0; i-- {
			/* guess the next quotient digit, quo_est, by dividing the first
			   two remaining dividend digits by the high order quotient digit.
			   quo_est is never low and is at most 2 high.  */
			var tmp uint64

			num_hi_sig = i + den_hi_sig + 1
			work = uint64(num[num_hi_sig])*base() + uint64(num[num_hi_sig-1])
			if num[num_hi_sig] != den[den_hi_sig] {
				quo_est = work / uint64(den[den_hi_sig])
			} else {
				quo_est = base() - 1
			}

			/* refine quo_est so it's usually correct, and at most one high.   */
			tmp = work - quo_est*uint64(den[den_hi_sig])
			if tmp < base() && uint64(den[den_hi_sig-1])*quo_est > (tmp*base()+uint64(num[num_hi_sig-2])) {
				quo_est--
			}

			/* Try QUO_EST as the quotient digit, by multiplying the
			   divisor by QUO_EST and subtracting from the remaining dividend.
			   Keep in mind that QUO_EST is the I - 1st digit.  */

			carry = 0
			for j = 0; j <= den_hi_sig; j++ {
				work = quo_est*uint64(den[j]) + carry
				carry = highPart(work)
				work = uint64(num[i+j]) - lowPart(work)
				num[i+j] = int64(lowPart(work))
				if highPart(work) != 0 {
					carry = carry + 1
				} else {
					carry = 0
				}
			}

			/* if quo_est was high by one, then num[i] went negative and
			   we need to correct things.  */

			if uint64(num[num_hi_sig]) < carry {
				quo_est--
				carry = 0 /* add divisor back in */
				for j = 0; j <= den_hi_sig; j++ {
					work = uint64(num[i+j]) + uint64(den[j]) + carry
					carry = highPart(work)
					num[i+j] = int64(lowPart(work))
				}
				num[num_hi_sig] += int64(carry)
			}

			/* store the quotient digit.  */
			quo[i] = int64(quo_est)
		}
	}

	decodeNum(quo[:4], lquo, hquo)

finish_up:
	/* if result is negative, make it so.  */
	if quo_neg != 0 {
		neg_double(*lquo, *hquo, lquo, hquo)
	}

	/* compute trial remainder:  rem = num - (quo * den)  */
	mul_double(*lquo, *hquo, lden_orig, hden_orig, lrem, hrem)
	neg_double(*lrem, *hrem, lrem, hrem)
	add_double(lnum_orig, hnum_orig, *lrem, *hrem, lrem, hrem)

	return overflow
}


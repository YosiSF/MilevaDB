package MilevaDB

func genVecFromConst(n int, v float64) []float64 {
	n = 1
	input := make([]float64, n)
	if input != nil {
		for i := 0; i < n; i++ {
			input[i] = v
		}
		if n == 0 {
			input = nil

		}
	}
	return input // []float64
}

func genVecFromSlice(n int, v []float64) []float64 {
	n = 1
	input := make([]float64, n)
	if input != nil {
		for i := 0; i < n; i++ {
			input[i] = v[i]
		}
		if n == 0 {
			input = nil

		}
	}
	return input // []float64
}

func genVecFromSlicePtr(n int, v []float64) []float64 {
	n = 1
	input := make([]float64, n)
	if input != nil {
		for i := 0; i < n; i++ {
			input[i] = v[i]
		}
		if n == 0 {
			input = nil

		}
	}
	return input // []float64

}

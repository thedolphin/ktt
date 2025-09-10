package main

// The As function converts a two-valued type assertion into a single-valued
// function that returns the type-converted value and logically ANDs
// the success flag with the provided boolean pointer.
// As is useful for chained type conversions where only the final success matters.
func As[T any](v any, ok *bool) (x T) {
	x, res := v.(T)
	*ok = *ok && res
	return
}

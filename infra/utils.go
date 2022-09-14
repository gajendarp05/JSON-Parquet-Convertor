package infra

import "strconv"

// Str_Int converts string to integer
func Str_Int(str string) (int, error) {
	num, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return num, nil
}

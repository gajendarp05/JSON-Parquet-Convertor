package infra

import "strconv"

// Str_Int converts string to integer
func Str_Int(str string) (int, error) {
	num, err := strconv.Atoi(str)
	if err != nil {
		return 0, err
	}
	return num, nil
}

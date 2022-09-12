package infra

import (
	"fmt"
	"os"
)

func CheckEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		panic(fmt.Sprintf("Not able to find %s in environment", key))
	}
	return val
}

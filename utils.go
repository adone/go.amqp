package amqp

import (
	"crypto/rand"
	"math/big"
	"unicode"
)

// SecureSrting returns random string with provided length
func SecureString(length int) string {
	var (
		generated = big.NewInt(0)
		max       = big.NewInt(130)
		result    = make([]byte, length)
	)

	for index := range result {
		generated, _ = rand.Int(rand.Reader, max)
		r := rune(generated.Int64())
		for !(unicode.IsNumber(r) || unicode.IsLetter(r)) {
			generated, _ = rand.Int(rand.Reader, max)
			r = rune(generated.Int64())
		}

		result[index] = byte(generated.Int64())
	}

	return string(result)
}

package util

import (
	"math/rand"
	"time"
)

var randSrc = rand.NewSource(time.Now().UnixNano())

const alpha = `abcdefghijklmnopqrstuvwxyz` +
	`ABCDEFGHIJKLMNOPQRSTUVWXYZ`
const num = `0123456789`
const charset = alpha + num

// GenerateRandStr generates random string
func GenerateRandStr(l int) string {
	r := rand.New(randSrc)
	b := make([]byte, l)
	for i := range b {
		if i == 0 {
			b[i] = alpha[r.Intn(len(alpha))]
			continue
		}
		b[i] = charset[r.Intn(len(charset))]
	}
	return string(b)
}

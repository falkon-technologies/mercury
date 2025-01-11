package mercury

import "math/rand"

// pid is alphanumerical with 8 characters, letters away in uppercase

const allowedChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// GeneratePID generates a PID
func GeneratePID() PID {
	var length = 8

	var pid = make([]byte, length)
	for i := 0; i < length; i++ {
		pid[i] = allowedChars[rand.Intn(len(allowedChars))]
	}

	return PID(pid)
}

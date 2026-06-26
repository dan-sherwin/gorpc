package gorpc

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
)

// Auth configures optional connection authentication.
type Auth struct {
	sharedSecret []byte
	enabled      bool
}

const authMethodHMACSHA256 = "hmac-sha256"

// SharedSecret enables HMAC-SHA256 challenge/response authentication.
func SharedSecret(secret string) Auth {
	if secret == "" {
		panic("gorpc: shared secret is empty")
	}

	return Auth{
		sharedSecret: []byte(secret),
		enabled:      true,
	}
}

func (a Auth) enabledAuth() bool {
	return a.enabled
}

func (a Auth) method() string {
	if !a.enabled {
		return ""
	}

	return authMethodHMACSHA256
}

func (a Auth) challenge() ([]byte, error) {
	challenge := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, challenge); err != nil {
		return nil, fmt.Errorf("create auth challenge: %w", err)
	}

	return challenge, nil
}

func (a Auth) sign(challenge []byte, protocolVersion uint16, codec string, clientName string) []byte {
	h := hmac.New(sha256.New, a.sharedSecret)
	_, _ = h.Write([]byte("gorpc-auth-v1"))

	var version [2]byte
	binary.BigEndian.PutUint16(version[:], protocolVersion)
	_, _ = h.Write(version[:])

	writeAuthString(h, codec)
	writeAuthString(h, clientName)
	_, _ = h.Write(challenge)

	return h.Sum(nil)
}

func (a Auth) verify(challenge []byte, protocolVersion uint16, codec string, clientName string, signature []byte) bool {
	expected := a.sign(challenge, protocolVersion, codec, clientName)
	return hmac.Equal(expected, signature)
}

func writeAuthString(w io.Writer, value string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = w.Write(length[:])
	_, _ = w.Write([]byte(value))
}

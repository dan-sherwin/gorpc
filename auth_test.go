package gorpc

import "testing"

func TestAuthBindsNegotiatedCapabilities(t *testing.T) {
	auth := SharedSecret("test-secret")
	challenge := []byte("test-challenge")
	legacy := auth.sign(challenge, ProtocolVersion, CodecMessagePack, "client")
	credit := auth.sign(challenge, ProtocolVersion, CodecMessagePack, "client", capabilityStreamCredit)
	if !auth.verify(challenge, ProtocolVersion, CodecMessagePack, "client", legacy) {
		t.Fatal("legacy proof failed verification")
	}
	if !auth.verify(challenge, ProtocolVersion, CodecMessagePack, "client", credit, capabilityStreamCredit) {
		t.Fatal("capability proof failed verification")
	}
	if auth.verify(challenge, ProtocolVersion, CodecMessagePack, "client", legacy, capabilityStreamCredit) {
		t.Fatal("legacy proof accepted with different capabilities")
	}
	if auth.verify(challenge, ProtocolVersion, CodecMessagePack, "client", credit) {
		t.Fatal("capability proof accepted with different capabilities")
	}
}

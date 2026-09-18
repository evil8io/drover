package telemetry

import (
	"context"
	"testing"
)

func TestSetupEmptyEndpointIsNoop(t *testing.T) {
	t.Parallel()
	tel, err := Setup(context.Background(), Config{ServiceName: "drover", Version: "test"})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if tel == nil {
		t.Fatal("Setup returned a nil Telemetry")
	}
	if err := tel.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown: %v", err)
	}
}

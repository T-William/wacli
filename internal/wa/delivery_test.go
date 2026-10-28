package wa

import (
	"testing"

	"go.mau.fi/whatsmeow"
)

func TestApplyDeliveryGuaranteesEnablesReplayOfRefusedEvents(t *testing.T) {
	cli := &whatsmeow.Client{}
	applyDeliveryGuarantees(cli)
	if !cli.EnableDecryptedEventBuffer {
		t.Fatal("without the decrypted-event buffer a refused message is dropped on redelivery (ErrOldCounter)")
	}
	if !cli.AutomaticMessageRerequestFromPhone {
		t.Fatal("undecryptable messages are not re-requested from the phone")
	}
}

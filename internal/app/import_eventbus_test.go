package app

import (
	"context"
	"testing"

	platformeventbus "github.com/lihongjie0209/microservice-platform-go/eventbus"
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	"google.golang.org/protobuf/proto"
)

type importProcessorStub struct{ validated, applied string }

func (p *importProcessorStub) Validate(_ context.Context, tenantID, id string) error {
	p.validated = tenantID + "/" + id
	return nil
}
func (p *importProcessorStub) Apply(_ context.Context, tenantID, id string) error {
	p.applied = tenantID + "/" + id
	return nil
}

func TestImportEventRuntimeRoutesValidationAndApply(t *testing.T) {
	processor := &importProcessorStub{}
	runtime := &importEventRuntime{worker: processor}
	for _, change := range []string{"requested", "apply-requested"} {
		payload, err := proto.Marshal(&importv1.ImportJobChangedEvent{Job: &importv1.ImportJob{Id: "job-1", TenantId: "tenant-1"}, ChangeType: change})
		if err != nil {
			t.Fatal(err)
		}
		envelope, err := platformeventbus.NewEnvelope(platformeventbus.Metadata{EventID: "event-" + change, EventType: "platform.import.v1.ImportJobChanged", AggregateID: "job-1", AggregateType: "import_job", TenantID: "tenant-1", SchemaVersion: 1}, &importv1.ImportJobChangedEvent{})
		if err != nil {
			t.Fatal(err)
		}
		envelope.Payload = payload
		if err := runtime.handle(context.Background(), envelope, change); err != nil {
			t.Fatal(err)
		}
	}
	if processor.validated != "tenant-1/job-1" || processor.applied != "tenant-1/job-1" {
		t.Fatalf("processor=%+v", processor)
	}
}

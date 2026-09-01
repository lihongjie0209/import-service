package app

import (
	"context"
	"testing"

	platformeventbus "github.com/lihongjie0209/microservice-platform-go/eventbus"
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	"google.golang.org/protobuf/proto"
)

type importProcessorStub struct{ validated, applied string }

func (p *importProcessorStub) Validate(_ context.Context, tenantID, applicationID, id string) error {
	p.validated = tenantID + "/" + applicationID + "/" + id
	return nil
}
func (p *importProcessorStub) Apply(_ context.Context, tenantID, applicationID, id string) error {
	p.applied = tenantID + "/" + applicationID + "/" + id
	return nil
}

func TestImportEventRuntimeRoutesValidationAndApply(t *testing.T) {
	processor := &importProcessorStub{}
	runtime := &importEventRuntime{worker: processor}
	for _, change := range []string{"requested", "apply-requested"} {
		payload, err := proto.Marshal(&importv1.ImportJobChangedEvent{Job: &importv1.ImportJob{Id: "job-1", TenantId: "tenant-1", ApplicationId: "app-1"}, ChangeType: change})
		if err != nil {
			t.Fatal(err)
		}
		envelope, err := platformeventbus.NewEnvelope(platformeventbus.Metadata{EventID: "event-" + change, EventType: "platform.import.v1.ImportJobChanged", AggregateID: "job-1", AggregateType: "import_job", TenantID: "tenant-1", ApplicationID: "app-1", SchemaVersion: 1}, &importv1.ImportJobChangedEvent{})
		if err != nil {
			t.Fatal(err)
		}
		envelope.Payload = payload
		if err := runtime.handle(context.Background(), envelope, change); err != nil {
			t.Fatal(err)
		}
	}
	if processor.validated != "tenant-1/app-1/job-1" || processor.applied != "tenant-1/app-1/job-1" {
		t.Fatalf("processor=%+v", processor)
	}
}

func TestImportEventRuntimeRejectsScopeMismatch(t *testing.T) {
	payload, err := proto.Marshal(&importv1.ImportJobChangedEvent{Job: &importv1.ImportJob{Id: "job-1", TenantId: "tenant-1", ApplicationId: "app-2"}, ChangeType: "requested"})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := platformeventbus.NewEnvelope(platformeventbus.Metadata{EventID: "event-1", EventType: "platform.import.v1.ImportJobChanged", AggregateID: "job-1", AggregateType: "import_job", TenantID: "tenant-1", ApplicationID: "app-1", SchemaVersion: 1}, &importv1.ImportJobChangedEvent{})
	if err != nil {
		t.Fatal(err)
	}
	envelope.Payload = payload
	if err := (&importEventRuntime{worker: &importProcessorStub{}}).handle(context.Background(), envelope, "requested"); err == nil {
		t.Fatal("expected scope mismatch")
	}
}

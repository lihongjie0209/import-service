package importjob

import (
	"time"

	"github.com/google/uuid"
	platformevents "github.com/lihongjie0209/microservice-platform-go/eventbus"
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	"google.golang.org/protobuf/proto"
)

func jobChangedEvent(job Job, change, actor string, at time.Time) (OutboxEvent, error) {
	payload := &importv1.ImportJobChangedEvent{Job: ToProto(job), ChangeType: change}
	envelope, err := platformevents.NewEnvelope(platformevents.Metadata{EventID: uuid.NewString(), EventType: "platform.import.v1.ImportJobChanged", AggregateID: job.ID, AggregateType: "import_job", TenantID: job.TenantID, ApplicationID: job.ApplicationID, SchemaVersion: 1, ActorID: actor, OccurredAt: at}, payload)
	if err != nil {
		return OutboxEvent{}, err
	}
	encoded, err := proto.Marshal(envelope)
	if err != nil {
		return OutboxEvent{}, err
	}
	return OutboxEvent{ID: envelope.GetEventId(), Subject: "platform.import.job." + change + ".v1", Envelope: encoded, AvailableAt: at, CreatedAt: at, UpdatedAt: at, CreatedBy: actor, UpdatedBy: actor}, nil
}

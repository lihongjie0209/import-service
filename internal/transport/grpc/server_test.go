package grpctransport

import (
	"testing"
	"time"

	"github.com/lihongjie0209/import-service/internal/auth"
	"github.com/lihongjie0209/import-service/internal/config"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
	importv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/import/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestImportGRPCRequirementCoversEveryBusinessMethod(t *testing.T) {
	t.Parallel()
	resolve := importGRPCRequirement(true)
	methods := []string{importv1.ImportService_ListImportDatasets_FullMethodName, importv1.ImportService_DescribeAvailableImportDataset_FullMethodName, importv1.ImportService_CreateImportJob_FullMethodName, importv1.ImportService_CompleteUpload_FullMethodName, importv1.ImportService_GetImportJob_FullMethodName, importv1.ImportService_ListImportJobs_FullMethodName, importv1.ImportService_CancelImportJob_FullMethodName, importv1.ImportService_RetryImportJob_FullMethodName, importv1.ImportService_ConfirmImportJob_FullMethodName, importv1.ImportService_CreateErrorReportDownloadURL_FullMethodName}
	for _, method := range methods {
		requirement, ok := resolve(method)
		if !ok || requirement.Resource == "" || requirement.Action == "" {
			t.Fatalf("method %q requirement = %+v, %v", method, requirement, ok)
		}
	}
	if _, ok := importGRPCRequirement(false)(importv1.ImportService_GetImportJob_FullMethodName); ok {
		t.Fatal("disabled authorization must not enforce")
	}
}

func TestAuthenticateGRPCPSKWildcard(t *testing.T) {
	t.Parallel()
	const key = "01234567890123456789012345678901"
	authService := auth.New(config.Config{JWT: config.JWT{Issuer: "test", Secret: key, TTL: time.Hour}})
	cfg := config.Auth{SkipGRPCMethods: []string{"/platform.import.v1.ImportService/*"}, PSK: config.PSK{Enabled: true, Key: key, GRPCMethods: []string{"/platform.import.v1.ImportService/*"}}}
	for _, test := range []struct {
		name, header string
		code         codes.Code
	}{{name: "valid", header: "PSK " + key, code: codes.OK}, {name: "psk precedes skip", code: codes.Unauthenticated}, {name: "bearer rejected", header: "Bearer " + key, code: codes.Unauthenticated}} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", test.header))
			authenticated, err := authenticateGRPC(ctx, "/platform.import.v1.ImportService/GetImportJob", authService, cfg)
			if got := status.Code(err); got != test.code {
				t.Fatalf("status code=%s want=%s", got, test.code)
			}
			if test.code == codes.OK {
				value, ok := platformprincipal.FromContext(authenticated)
				if !ok || value.ID != "import-service:psk" || value.Type != platformprincipal.TypeServiceAccount {
					t.Fatalf("principal=%#v ok=%v", value, ok)
				}
			}
		})
	}
}

func TestAuthenticateGRPCJWTInjectsSharedPrincipal(t *testing.T) {
	t.Parallel()
	const key = "01234567890123456789012345678901"
	service := auth.New(config.Config{JWT: config.JWT{Issuer: "test", Secret: key, TTL: time.Hour}, Auth: config.Auth{ClientID: "client", ClientSecret: "secret"}})
	token, err := service.Issue("user-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx := metadata.NewIncomingContext(t.Context(), metadata.Pairs("authorization", "Bearer "+token))
	ctx, err = authenticateGRPC(ctx, "/platform.import.v1.ImportService/GetImportJob", service, config.Auth{})
	if err != nil {
		t.Fatal(err)
	}
	value, ok := platformprincipal.FromContext(ctx)
	if !ok || value.ID != "user-1" || value.Type != platformprincipal.TypeUser {
		t.Fatalf("principal=%#v ok=%v", value, ok)
	}
}

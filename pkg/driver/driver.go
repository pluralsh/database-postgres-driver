package driver

import (
	"context"
	"errors"

	"github.com/pluralsh/database-postgres-driver/pkg/postgres"
	"k8s.io/klog"

	databasespec "github.com/pluralsh/database-interface-api/spec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func NewDriver(provisioner string, postgresDB *postgres.Postgres) (*IdentityServer, *ProvisionerServer) {
	return &IdentityServer{
			provisioner: provisioner,
		}, &ProvisionerServer{
			provisioner: provisioner,
			postgresDB:  postgresDB,
		}
}

type ProvisionerServer struct {
	provisioner string
	postgresDB  *postgres.Postgres
}

func (ps *ProvisionerServer) DriverCreateDatabase(_ context.Context, req *databasespec.DriverCreateDatabaseRequest) (*databasespec.DriverCreateDatabaseResponse, error) {
	if ps.postgresDB == nil {
		return &databasespec.DriverCreateDatabaseResponse{}, status.Error(codes.Internal, "Database not initialized")
	}
	databaseName := req.GetName()
	klog.V(3).Info("Create Database", "name", databaseName)
	if err := ps.postgresDB.CreateDatabase(databaseName); err != nil {
		return &databasespec.DriverCreateDatabaseResponse{}, err
	}

	return &databasespec.DriverCreateDatabaseResponse{
		DatabaseId: databaseName,
	}, nil
}

func (ps *ProvisionerServer) DriverDeleteDatabase(_ context.Context, req *databasespec.DriverDeleteDatabaseRequest) (*databasespec.DriverDeleteDatabaseResponse, error) {
	if ps.postgresDB == nil {
		return &databasespec.DriverDeleteDatabaseResponse{}, status.Error(codes.Internal, "Database not initialized")
	}
	if err := ps.postgresDB.DeleteDatabase(req.DatabaseId); err != nil {
		return &databasespec.DriverDeleteDatabaseResponse{}, err
	}

	return &databasespec.DriverDeleteDatabaseResponse{}, nil
}

// This call grants access to an account. The account_name in the request shall be used as a unique identifier to create credentials.
// The account_id returned in the response will be used as the unique identifier for deleting this access when calling DriverRevokeDatabaseAccess.
func (ps *ProvisionerServer) DriverGrantDatabaseAccess(_ context.Context, req *databasespec.DriverGrantDatabaseAccessRequest) (*databasespec.DriverGrantDatabaseAccessResponse, error) {
	resp := &databasespec.DriverGrantDatabaseAccessResponse{
		AccountId:   req.DatabaseId,
		Credentials: map[string]*databasespec.CredentialDetails{},
	}
	resp.Credentials["cred"] = &databasespec.CredentialDetails{Secrets: map[string]string{
		"DB_USER":     ps.postgresDB.User,
		"DB_PASSWORD": ps.postgresDB.Password,
		"DB_HOST":     ps.postgresDB.Host,
		"DB_PORT":     string(ps.postgresDB.Port),
	}}

	return resp, nil
}

// This call revokes all access to a particular database from a principal.
func (ps *ProvisionerServer) DriverRevokeDatabaseAccess(context.Context, *databasespec.DriverRevokeDatabaseAccessRequest) (*databasespec.DriverRevokeDatabaseAccessResponse, error) {
	return &databasespec.DriverRevokeDatabaseAccessResponse{}, nil
}

type IdentityServer struct {
	provisioner string
}

func (id *IdentityServer) DriverGetInfo(context.Context, *databasespec.DriverGetInfoRequest) (*databasespec.DriverGetInfoResponse, error) {
	if id.provisioner == "" {
		klog.Error(errors.New("provisioner name cannot be empty"), "Invalid argument")
		return nil, status.Error(codes.InvalidArgument, "ProvisionerName is empty")
	}

	return &databasespec.DriverGetInfoResponse{
		Name: id.provisioner,
	}, nil
}

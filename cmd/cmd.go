package main

import (
	"context"
	"flag"
	"strings"

	"github.com/pluralsh/database-interface-controller/pkg/provisioner"
	"github.com/pluralsh/database-postgres-driver/pkg/driver"
	"github.com/pluralsh/database-postgres-driver/pkg/postgres"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"k8s.io/klog"
)

const provisionerName = "fake.database.plural.sh"

var (
	driverAddress = "unix:///var/lib/database/database.sock"
	dbUser        = "postgres"
	dbPassword    = ""
	dbHost        = ""
	dbPort        = 5432
)

var cmd = &cobra.Command{
	Use:           "postgres-database-driver",
	Short:         "K8s database driver for Postgres database",
	SilenceErrors: true,
	SilenceUsage:  true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return run(cmd.Context(), args)
	},
	DisableFlagsInUseLine: true,
}

func init() {
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	flag.Set("alsologtostderr", "true")
	kflags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(kflags)

	persistentFlags := cmd.PersistentFlags()
	persistentFlags.AddGoFlagSet(kflags)

	stringFlag := persistentFlags.StringVarP
	intFlag := persistentFlags.IntVarP

	stringFlag(&driverAddress,
		"driver-addr",
		"d",
		driverAddress,
		"path to unix domain socket where driver should listen")
	stringFlag(&dbUser,
		"db-user",
		"",
		dbUser,
		"postgres user")
	stringFlag(&dbPassword,
		"db-password",
		"",
		dbPassword,
		"postgres password")
	stringFlag(&dbHost,
		"db-host",
		"",
		dbHost,
		"postgres host name")
	intFlag(&dbPort,
		"db-port",
		"",
		dbPort,
		"postgres port number")

	viper.BindPFlags(cmd.PersistentFlags())
	cmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		if viper.IsSet(f.Name) && viper.GetString(f.Name) != "" {
			cmd.PersistentFlags().Set(f.Name, viper.GetString(f.Name))
		}
	})
}

func run(ctx context.Context, args []string) error {
	postgresDB := &postgres.Postgres{
		User:     dbUser,
		Password: dbPassword,
		Host:     dbHost,
		Port:     dbPort,
	}
	identityServer, databaseProvisioner := driver.NewDriver(provisionerName, postgresDB)
	server, err := provisioner.NewDefaultProvisionerServer(driverAddress,
		identityServer,
		databaseProvisioner)
	if err != nil {
		return err
	}
	return server.Run(ctx)
}

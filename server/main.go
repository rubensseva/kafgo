package main

import (
	"os"
	"fmt"
	"net"

	"github.com/rubensseva/kafgo/proto"

	"google.golang.org/grpc"
	"gorm.io/driver/sqlite" // Sqlite driver based on GGO
	"gorm.io/gorm"
)

var (
	db *gorm.DB
)

func main() {
	var err error
	db, err = gorm.Open(sqlite.Open("gorm.db"), &gorm.Config{})
	if err != nil {
		fmt.Printf("error when connecting to sqlite: %v\n", err)
		os.Exit(1)
	}

	sqlDB, err := db.DB()
	if err != nil {
		fmt.Printf("Getting underlying sql db instance: %v\n", err)
		os.Exit(1)
	}


	// Need to enforce only 1 connection for SQLite to work properly with Gorm
	// SetMaxIdleConns sets the maximum number of connections in the idle connection pool.
	sqlDB.SetMaxIdleConns(1)

	// SetMaxOpenConns sets the maximum number of open connections to the database.
	sqlDB.SetMaxOpenConns(1)

	merr := db.AutoMigrate(&Msg{})
	if merr != nil {
		fmt.Printf("Error when automigrating %v\n", merr)
		os.Exit(1)
	}


	lis, err := net.Listen("tcp", "localhost:5000")
	if err != nil {
		fmt.Printf("failed to listen: %v\n", err)
	}

	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	server := &KafgoServer{}
	proto.RegisterKafgoServer(grpcServer, server)

	fmt.Printf("server listening at %v\n", lis.Addr())
	if err := grpcServer.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v\n", err)
		os.Exit(1)
	}
}

package main

import (
	"flag"
	"fmt"
	"github.com/goat-project/goat/importer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"net"
)

// CLI option names
var (
	ip           = flag.String("listen-ip", "127.0.0.1", "IP address to bind to")
	port         = flag.Uint("port", 9623, "port to bind to")
	templatesDir = flag.String("templates-dir", "~/.goat/templates/", "templates directory")
	outputDir    = flag.String("output-dir", "~/goat-out", "output directory")
	tls          = flag.Bool("tls", false, "True uses TLS, false uses plaintext TCP")
	certFile     = flag.String("cert-file", "server.pem", "server certificate file")
	keyFile      = flag.String("key-file", "server.key", "server key file")
)

func startServer() error {
	server, err := net.Listen("tcp", fmt.Sprintf("%s:%d", *ip, *port))
	if err != nil {
		return err
	}

	var opts []grpc.ServerOption
	if *tls {
		if *certFile == "" {
			return fmt.Errorf("Please specify a -cert-file")
		}
		if *keyFile == "" {
			return fmt.Errorf("Please specify a -key-file")
		}
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			return err
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}

	grpcServer := grpc.NewServer(opts...)
	acceptAll := func(_ string) bool {
		return true
	}

	importer.NewAccountingServiceImpl(acceptAll)
	return grpcServer.Serve(server)
}

func main() {
	if flag.NFlag()+flag.NArg() == 0 {
		flag.PrintDefaults()
		return
	}
	flag.Parse()
	err := startServer()
	if err != nil {
		log.Fatal(err)
	}
}

package main

import (
	"flag"
	"log"
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
	return nil
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

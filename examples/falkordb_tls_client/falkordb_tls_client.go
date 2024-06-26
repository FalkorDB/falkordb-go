package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/FalkorDB/falkordb-go"
)

var (
	tlsCertFile   = flag.String("tls-cert-file", "falkordb.crt", "A a X.509 certificate to use for authenticating the server to connected clients, masters or cluster peers. The file should be PEM formatted.")
	tlsKeyFile    = flag.String("tls-key-file", "falkordb.key", "A a X.509 privat ekey to use for authenticating the server to connected clients, masters or cluster peers. The file should be PEM formatted.")
	tlsCaCertFile = flag.String("tls-ca-cert-file", "ca.crt", "A PEM encoded CA's certificate file.")
	host          = flag.String("host", "127.0.0.1:6379", "FalkorDB host.")
	password      = flag.String("password", "", "FalkorDB password.")
)

func exists(filename string) (exists bool) {
	exists = false
	info, err := os.Stat(filename)
	if os.IsNotExist(err) || info.IsDir() {
		return
	}
	exists = true
	return
}

/*
 * Example of how to establish an SSL connection from your app to the FalkorDB Server
 */
func main() {
	flag.Parse()
	// Quickly check if the files exist
	if !exists(*tlsCertFile) || !exists(*tlsKeyFile) || !exists(*tlsCaCertFile) {
		fmt.Println("Some of the required files does not exist. Leaving example...")
		return
	}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(*tlsCertFile, *tlsKeyFile)
	if err != nil {
		log.Fatal(err)
	}

	// Load CA cert
	caCert, err := os.ReadFile(*tlsCaCertFile)
	if err != nil {
		log.Fatal(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	clientTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	// InsecureSkipVerify controls whether a client verifies the
	// server's certificate chain and host name.
	// If InsecureSkipVerify is true, TLS accepts any certificate
	// presented by the server and any host name in that certificate.
	// In this mode, TLS is susceptible to man-in-the-middle attacks.
	// This should be used only for testing.
	clientTLSConfig.InsecureSkipVerify = true

	db, _ := falkordb.FalkorDBNew(&falkordb.ConnectionOption{
		Addr:      *host,
		Password:  *password,
		TLSConfig: clientTLSConfig,
	})

	graph := db.SelectGraph("social")

	q := "CREATE (w:WorkPlace {name:'FalkorDB'}) RETURN w"
	res, _ := graph.Query(q, nil, nil)

	res.Next()
	r := res.Record()
	w := r.GetByIndex(0).(*falkordb.Node)
	fmt.Println(w.Labels[0])
	// Output: WorkPlace
}

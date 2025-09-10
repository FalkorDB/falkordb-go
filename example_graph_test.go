package falkordb_test

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/FalkorDB/falkordb-go"
)

func ExampleFalkorDB_SelectGraph() {
	db, _ := falkordb.FalkorDBNew(&falkordb.ConnectionOption{Addr: "0.0.0.0:6379"})

	graph := db.SelectGraph("social")

	q := "CREATE (w:WorkPlace {name:'FalkorDB'}) RETURN w"
	res, _ := graph.Query(q, nil, nil)

	res.Next()
	r := res.Record()
	w := r.GetByIndex(0).(*falkordb.Node)
	fmt.Println(w.Labels[0])
	// Output: WorkPlace
}

func ExampleFalkorDBNew_tls() {
	// Consider the following helper methods that provide us with the connection details (host and password)
	// and the paths for:
	//     tls_cert - A a X.509 certificate to use for authenticating the  server to connected clients, masters or cluster peers. The file should be PEM formatted
	//     tls_key - A a X.509 private key to use for authenticating the  server to connected clients, masters or cluster peers. The file should be PEM formatted
	//	   tls_cacert - A PEM encoded CA's certificate file
	host, password := getConnectionDetails()
	tlsready, tls_cert, tls_key, tls_cacert := getTLSdetails()

	// Skip if we dont have all files to properly connect
	if tlsready == false {
		return
	}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(tls_cert, tls_key)
	if err != nil {
		log.Fatal(err)
	}

	// Load CA cert
	caCert, err := ioutil.ReadFile(tls_cacert)
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

	db, err := falkordb.FalkorDBNew(&falkordb.ConnectionOption{
		Addr:      host,
		Password:  password,
		TLSConfig: clientTLSConfig,
	})
	graph := db.SelectGraph("social")

	q := "CREATE (w:WorkPlace {name:'FalkorDB'}) RETURN w"
	res, _ := graph.Query(q, nil, nil)

	res.Next()
	r := res.Record()
	w := r.GetByIndex(0).(*falkordb.Node)
	fmt.Println(w.Labels[0])
}

func getConnectionDetails() (host string, password string) {
	value, exists := os.LookupEnv("FALKORDB_TEST_HOST")
	host = "localhost:6379"
	if exists && value != "" {
		host = value
	}
	password = ""
	valuePassword, existsPassword := os.LookupEnv("FALKORDB_TEST_PASSWORD")
	if existsPassword && valuePassword != "" {
		password = valuePassword
	}
	return
}

func getTLSdetails() (tlsready bool, tls_cert string, tls_key string, tls_cacert string) {
	tlsready = false
	value, exists := os.LookupEnv("TLS_CERT")
	if exists && value != "" {
		info, err := os.Stat(value)
		if os.IsNotExist(err) || info.IsDir() {
			return
		}
		tls_cert = value
	} else {
		return
	}
	value, exists = os.LookupEnv("TLS_KEY")
	if exists && value != "" {
		info, err := os.Stat(value)
		if os.IsNotExist(err) || info.IsDir() {
			return
		}
		tls_key = value
	} else {
		return
	}
	value, exists = os.LookupEnv("TLS_CACERT")
	if exists && value != "" {
		info, err := os.Stat(value)
		if os.IsNotExist(err) || info.IsDir() {
			return
		}
		tls_cacert = value
	} else {
		return
	}
	tlsready = true
	return
}

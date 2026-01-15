package falkordb_test

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"

	"github.com/FalkorDB/falkordb-go/v2"
)

func ExampleFalkorDB_SelectGraph() {
	db, _ := falkordb.FalkorDBNew(&falkordb.ConnectionOption{Addr: "0.0.0.0:6379"})

	graph := db.SelectGraph("social")

	q := "CREATE (w:WorkPlace {name:'FalkorDB'}) RETURN w"
	res, _ := graph.Query(q, nil, nil)

	res.Next()
	r := res.Record()
	wIface, _ := r.GetByIndex(0)
	w := wIface.(*falkordb.Node)

	fmt.Println(w.Labels[0])
	// Output: WorkPlace
}

func ExampleFalkorDBNew() {
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
	caCert, err := os.ReadFile(tls_cacert)
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
	wIface, _ := r.GetByIndex(0)
	w := wIface.(*falkordb.Node)
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

func ExampleFalkorDB_UDFLoad() {
	db, _ := falkordb.FalkorDBNew(&falkordb.ConnectionOption{Addr: "0.0.0.0:6379"})
	defer db.Conn.Close()

	// Define a simple UDF library
	library := "StringUtils"
	source := `
		function UpperCaseOdd(s) {
			return s.split('').map((char, i) => (i % 2 !== 0 ? char.toUpperCase() : char)).join('');
		}
		falkor.register('UpperCaseOdd', UpperCaseOdd);
	`

	// Load the UDF library
	err := db.UDFLoad(library, source)
	if err != nil {
		log.Fatal(err)
	}

	// List all loaded UDF libraries
	udfs, err := db.UDFList()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Loaded UDFs: %v\n", udfs)

	// Use the UDF in a query
	graph := db.SelectGraph("demo")
	result, err := graph.Query("RETURN StringUtils.UpperCaseOdd('hello')", nil, nil)
	if err != nil {
		log.Fatal(err)
	}
	result.Next()
	r := result.Record()
	val, _ := r.GetByIndex(0)
	fmt.Println(val) // Expected: hElLo (characters at odd indices are uppercase)

	// Delete the UDF library
	err = db.UDFDelete(library)
	if err != nil {
		log.Fatal(err)
	}

	// Or flush all UDF libraries
	err = db.UDFFlush()
	if err != nil {
		log.Fatal(err)
	}
}

package fuq

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

func SetupTLSRootCA(c Config) (*tls.Config, error) {
	rootData, err := ioutil.ReadFile(c.RootCAFile)
	if err != nil {
		return nil, fmt.Errorf("Error loading TLS root CA cert: %v", err)
	}

	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(rootData); !ok {
		return nil, fmt.Errorf("Error adding Root CA certificate to pool")
	}
	return &tls.Config{
		RootCAs:    certPool,
		ServerName: c.CertName,
	}, nil
}

func SetupTLS(c Config) (*tls.Config, error) {
	config, err := SetupTLSRootCA(c)
	if err != nil {
		return nil, err
	}

	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("Error loading TLS data: %v", err)
	}

	config.Certificates = []tls.Certificate{cert}
	return config, nil
}

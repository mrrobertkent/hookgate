package natsconn

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"

	"github.com/mrrobertkent/hookgate/internal/config"
	"github.com/mrrobertkent/hookgate/internal/testutil"
)

type certPair struct{ cert, key []byte }

func issue(t *testing.T, tmpl, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (certPair, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if parent == nil {
		parent, parentKey = tmpl, key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	kder, _ := x509.MarshalECPrivateKey(key)
	return certPair{
		cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		key:  pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}),
	}, cert, key
}

func writeFile(t *testing.T, dir, name string, b []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// Mutual TLS with a private CA: the client verifies the server against tls.ca and presents tls.cert.
func TestMutualTLS(t *testing.T) {
	now := time.Now()
	caPEM, ca, caKey := issue(t, &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}, nil, nil)
	srvPEM, _, _ := issue(t, &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "nats"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, ca, caKey)
	cliPEM, _, _ := issue(t, &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "hookgate"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, ca, caKey)

	srvCert, err := tls.X509KeyPair(srvPEM.cert, srvPEM.key)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM.cert)
	s := testutil.RunServer(t, func(o *server.Options) {
		o.TLS, o.TLSVerify = true, true
		o.TLSConfig = &tls.Config{Certificates: []tls.Certificate{srvCert}, ClientCAs: pool,
			ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS12}
	})

	dir := t.TempDir()
	tlsCfg := &config.TLS{
		CA:   &config.SecretRef{File: writeFile(t, dir, "ca.pem", caPEM.cert)},
		Cert: &config.SecretRef{File: writeFile(t, dir, "cert.pem", cliPEM.cert)},
		Key:  &config.SecretRef{File: writeFile(t, dir, "key.pem", cliPEM.key)},
	}
	cfg := conn([]string{"tls://127.0.0.1:" + itoaPort(s)}, func(c *config.NATSConnection) { c.None = true; c.TLS = tlsCfg })
	c, err := Connect("test", cfg, discard, Callbacks{})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.WaitConnected(ctx); err != nil {
		t.Fatalf("not connected over mutual TLS: %v", err)
	}

	// A key that does not belong to the certificate is a configuration fault.
	otherPEM, _, _ := issue(t, &x509.Certificate{SerialNumber: big.NewInt(4), NotBefore: now, NotAfter: now.Add(time.Hour)}, ca, caKey)
	tlsCfg.Key = &config.SecretRef{File: writeFile(t, dir, "other.pem", otherPEM.key)}
	if _, err := Options(cfg); err == nil {
		t.Error("mismatched key pair accepted")
	}
}

func itoaPort(s *server.Server) string {
	_, port, _ := net.SplitHostPort(s.Addr().String())
	return port
}

// A creds file (user JWT and seed) is accepted from a file or from the environment.
func TestCredsOption(t *testing.T) {
	account, _ := nkeys.CreateAccount()
	accountPub, _ := account.PublicKey()
	user, _ := nkeys.CreateUser()
	userPub, _ := user.PublicKey()
	seed, _ := user.Seed()
	claims := jwt.NewUserClaims(userPub)
	claims.IssuerAccount = accountPub
	token, err := claims.Encode(account)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := jwt.FormatUserConfig(token, seed)
	if err != nil {
		t.Fatal(err)
	}
	file := writeFile(t, t.TempDir(), "user.creds", creds)
	t.Setenv("TEST_CREDS", string(creds))
	for _, ref := range []config.SecretRef{{File: file}, {Env: "TEST_CREDS"}} {
		if _, err := Options(config.NATSConnection{Creds: &ref}); err != nil {
			t.Errorf("%s: %v", ref, err)
		}
	}
}

func newCA(t *testing.T, serial int64, name string) (certPair, *x509.Certificate, *ecdsa.PrivateKey) {
	now := time.Now()
	return issue(t, &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign}, nil, nil)
}

func clientCert(t *testing.T, serial int64, ca *x509.Certificate, caKey *ecdsa.PrivateKey) certPair {
	now := time.Now()
	p, _, _ := issue(t, &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "hookgate"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}, ca, caKey)
	return p
}

// A server refusing the client certificate, and a server certificate the configured CA does not sign, are
// credential errors, not an unreachable broker.
func TestTLSRejectionsAreCredentialErrors(t *testing.T) {
	now := time.Now()
	caPEM, ca, caKey := newCA(t, 1, "test ca")
	otherPEM, other, otherKey := newCA(t, 2, "other ca")
	srvPEM, _, _ := issue(t, &x509.Certificate{SerialNumber: big.NewInt(3), Subject: pkix.Name{CommonName: "nats"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}, ca, caKey)
	srvCert, err := tls.X509KeyPair(srvPEM.cert, srvPEM.key)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEM.cert)
	s := testutil.RunServer(t, func(o *server.Options) {
		o.TLS, o.TLSVerify = true, true
		o.TLSConfig = &tls.Config{Certificates: []tls.Certificate{srvCert}, ClientAuth: tls.RequireAndVerifyClientCert,
			ClientCAs: pool, MinVersion: tls.VersionTLS12}
	})
	dir := t.TempDir()
	good, rogue := clientCert(t, 4, ca, caKey), clientCert(t, 5, other, otherKey)
	ref := func(name string, b []byte) *config.SecretRef {
		return &config.SecretRef{File: writeFile(t, dir, name, b)}
	}
	cases := map[string]*config.TLS{
		"client certificate refused": {CA: ref("ca.pem", caPEM.cert), Cert: ref("rogue.pem", rogue.cert), Key: ref("rogue.key", rogue.key)},
		"server certificate unknown": {CA: ref("other.pem", otherPEM.cert), Cert: ref("good.pem", good.cert), Key: ref("good.key", good.key)},
	}
	for name, tc := range cases {
		cfg := conn([]string{"tls://127.0.0.1:" + itoaPort(s)}, func(c *config.NATSConnection) { c.None = true; c.TLS = tc })
		failures := make(chan error, 16)
		c, err := Connect("test", cfg, discard, Callbacks{Sampled: func(_, cur Status) {
			if cur.Err == nil {
				return
			}
			select {
			case failures <- cur.Err:
			default:
			}
		}})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		select {
		case err := <-failures:
			if !IsCredentialError(err) {
				t.Errorf("%s: %v is not a credential error", name, err)
			}
			testutil.Eventually(t, 5*time.Second, func() bool { return c.Status().Rejected }, name+": status not rejected")
		case <-time.After(10 * time.Second):
			t.Errorf("%s: no failure reported", name)
		}
		c.Close()
	}
}

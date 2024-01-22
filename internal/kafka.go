package internal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Kafka struct {
	logger *logrus.Logger

	client *kgo.Client

	batchSize int

	ctx       context.Context
	ctxCancel context.CancelFunc

	wg sync.WaitGroup
}

func New(log *logrus.Logger) (*Kafka, error) {
	var (
		err  error
		opts []kgo.Opt
	)

	v := &Kafka{
		logger: log,
	}

	opts = []kgo.Opt{
		kgo.SeedBrokers([]string{
			"127.0.0.11:9093",
			"127.0.0.12:9093",
			"127.0.0.13:9093",
		}...),
		kgo.ConsumerGroup("test"),
		kgo.ConsumeTopics("TODO"),
		kgo.RequireStableFetchOffsets(),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.InstanceID("developer-101"),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.OnPartitionsAssigned(v.onPartitionsAssigned),
		kgo.OnPartitionsLost(v.onPartitionsLost),
		kgo.OnPartitionsRevoked(v.onPartitionsRevoked),
	}

	// comment if TLS disabled
	{
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair("./cert.pem", "./key.pem")
		if err != nil {
			return nil, err
		}

		certPool := x509.NewCertPool()
		for _, certFileName := range []string{"./ca-cert.pem"} {
			var certFile []byte
			if certFile, err = os.ReadFile(certFileName); err != nil {
				return nil, err
			}
			certPool.AppendCertsFromPEM(certFile)
		}
		tlsConfig.RootCAs = certPool
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if v.client, err = kgo.NewClient(opts...); err != nil {
		return nil, err
	}

	v.ctx, v.ctxCancel = context.WithCancel(context.Background())

	v.wg.Add(1)
	go v.mainLoop()

	return v, nil
}

func (v *Kafka) Log() *logrus.Logger {
	return v.logger
}

func (v *Kafka) Done() <-chan struct{} {
	return v.ctx.Done()
}

func (v *Kafka) Shutdown() {
	v.ctxCancel()
	v.wg.Wait()
	v.client.Close()
}

func (v *Kafka) mainLoop() {
	defer v.wg.Done()
	defer v.ctxCancel()
	defer v.client.AllowRebalance()

	for {
		select {
		case <-v.ctx.Done():
			return

		default:
			_ = v.client.PollRecords(v.ctx, 10)
			v.client.AllowRebalance()
		}
	}
}

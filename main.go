package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"

	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

func main() {
	envs := []string{
		"UPSTASH_USER",
		"UPSTASH_PASS",
		"UPSTASH_URL",
		"UPSTASH_TOPIC",
		"SHOPIFY_SHARED_SECRET",
	}

	for _, env := range envs {
		if os.Getenv(env) == "" {
			log.Fatalf("Error: missing %s", env)
		}
	}

	mechanism, err := scram.Mechanism(scram.SHA256, os.Getenv("UPSTASH_USER"), os.Getenv("UPSTASH_PASS"))
	if err != nil {
		log.Fatalln(err)
	}

	dialer := &kafka.Dialer{
		SASLMechanism: mechanism,
		TLS:           &tls.Config{},
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{os.Getenv("UPSTASH_URL")},
		GroupID: "$GROUP_NAME",
		Topic:   os.Getenv("UPSTASH_TOPIC"),
		Dialer:  dialer,
	})

	defer r.Close()

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}

		if !verify(m) {
			fmt.Println("message failed verification")
			continue
		}

		fmt.Printf("message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}

func verify(m kafka.Message) bool {
	key := os.Getenv("SHOPIFY_SHARED_SECRET")

	headers := map[string]string{}
	for _, header := range m.Headers {
		headers[header.Key] = string(header.Value)
	}

	shmac := headers["X-Shopify-Hmac-Sha256"]
	shop := headers["X-Shopify-Shop-Domain"]

	if shop == "" {
		return false
	}

	h := hmac.New(sha256.New, []byte(key))
	h.Write(m.Value)
	enc := base64.StdEncoding.EncodeToString(h.Sum(nil))

	return enc == shmac
}

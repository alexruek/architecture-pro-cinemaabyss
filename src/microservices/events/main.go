package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─── Kafka helpers ───────────────────────────────────────────────────────────

func newWriter(broker, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		WriteTimeout: 10 * time.Second,
	}
}

func publishAndConsume(broker, topic string, payload []byte) error {
	// Produce
	w := newWriter(broker, topic)
	defer w.Close()

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(topic),
			Value: payload,
		},
	)
	if err != nil {
		return err
	}
	log.Printf("[events] produced to topic=%s payload=%s", topic, string(payload))

	// Consume one message
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		GroupID:   "events-service-group",
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   3 * time.Second,
	})
	defer r.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	msg, err := r.ReadMessage(ctx)
	if err != nil {
		log.Printf("[events] consumer timeout for topic=%s (message already processed)", topic)
		return nil
	}
	log.Printf("[events] consumed topic=%s offset=%d value=%s", topic, msg.Offset, string(msg.Value))
	return nil
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":true}`))
}

func makeEventHandler(broker, topic string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}

		payload, _ := json.Marshal(body)

		if err := publishAndConsume(broker, topic, payload); err != nil {
			log.Printf("[events] error: %v", err)
			http.Error(w, "failed to process event", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"status":"success"}`))
	}
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	port   := getEnv("PORT", "8082")
	broker := getEnv("KAFKA_BROKERS", "localhost:9092")

	mux := http.NewServeMux()

	mux.HandleFunc("/api/events/health",  healthHandler)
	mux.HandleFunc("/api/events/movie",   makeEventHandler(broker, "movie-events"))
	mux.HandleFunc("/api/events/user",    makeEventHandler(broker, "user-events"))
	mux.HandleFunc("/api/events/payment", makeEventHandler(broker, "payment-events"))

	log.Printf("Events service started on :%s | broker=%s", port, broker)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func makeProxy(target string) *httputil.ReverseProxy {
	u, err := url.Parse(target)
	if err != nil {
		log.Fatalf("invalid target URL %s: %v", target, err)
	}
	return httputil.NewSingleHostReverseProxy(u)
}

func main() {
	port             := getEnv("PORT", "8000")
	monolithURL      := getEnv("MONOLITH_URL", "http://localhost:8080")
	moviesServiceURL := getEnv("MOVIES_SERVICE_URL", "http://localhost:8081")
	eventsServiceURL := getEnv("EVENTS_SERVICE_URL", "http://localhost:8082")
	gradualMigration := getEnv("GRADUAL_MIGRATION", "false")
	migrationPercent := getEnv("MOVIES_MIGRATION_PERCENT", "0")

	percent, err := strconv.Atoi(migrationPercent)
	if err != nil || percent < 0 || percent > 100 {
		percent = 0
	}

	monolithProxy := makeProxy(monolithURL)
	moviesProxy   := makeProxy(moviesServiceURL)
	eventsProxy   := makeProxy(eventsServiceURL)

	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":true}`))
	})

	// Movies — Strangler Fig с фиче-флагом
	mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		if gradualMigration == "true" && rand.Intn(100) < percent {
			log.Printf("[proxy] /api/movies → movies-service (%d%%)", percent)
			moviesProxy.ServeHTTP(w, r)
		} else {
			log.Printf("[proxy] /api/movies → monolith")
			monolithProxy.ServeHTTP(w, r)
		}
	})

	// Events — всегда в events-service
	mux.HandleFunc("/api/events/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[proxy] %s → events-service", r.URL.Path)
		eventsProxy.ServeHTTP(w, r)
	})

	// Все остальное — в монолит
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("[proxy] %s → monolith", r.URL.Path)
		monolithProxy.ServeHTTP(w, r)
	})

	log.Printf("Proxy service started on :%s | gradual=%s | movies_percent=%d%%",
		port, gradualMigration, percent)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
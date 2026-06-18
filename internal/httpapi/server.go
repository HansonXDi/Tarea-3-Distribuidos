package httpapi

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"tarea3/internal/protocol"
)

// Server expone los endpoints REST de un proceso expendedora. Recibe:
//   - POST /inventory      -> inventario inicial enviado por una réplica paralela
//   - POST /state-report   -> reporte final de inventario+vetos para el quórum
//   - GET  /health         -> chequeo simple de disponibilidad
//
// Los datos recibidos se entregan a través de canales para que la lógica de
// negocio (en el paquete process) decida qué hacer con ellos, manteniendo
// esta capa enfocada solo en transporte HTTP.
type Server struct {
	Port int

	InventoryCh chan protocol.InventoryPayload
	StateCh     chan protocol.StatePayload

	mu      sync.Mutex
	started bool
}

// NewServer crea un servidor HTTP para el puerto indicado.
// Entrada: puerto de escucha. Salida: *Server listo para iniciar.
func NewServer(port int) *Server {
	return &Server{
		Port:        port,
		InventoryCh: make(chan protocol.InventoryPayload, 64),
		StateCh:     make(chan protocol.StatePayload, 64),
	}
}

// Start registra los handlers y arranca el servidor HTTP en una goroutine.
// Entrada: ninguna. Salida: ninguna (no bloquea; loguea si el servidor falla).
func (s *Server) Start() {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	mux := http.NewServeMux()
	mux.HandleFunc("/inventory", s.handleInventory)
	mux.HandleFunc("/state-report", s.handleStateReport)
	mux.HandleFunc("/health", s.handleHealth)

	addr := fmt.Sprintf(":%d", s.Port)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatalf("[HTTP] Error al escuchar en %s: %v", addr, err)
		}
	}()
}

// handleInventory procesa POST /inventory: una réplica paralela envía el
// inventario inicial recién sorteado para que este proceso lo guarde.
// Entrada/Salida HTTP: JSON InventoryPayload -> JSON AckResponse.
func (s *Server) handleInventory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload protocol.InventoryPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "json inválido", http.StatusBadRequest)
		return
	}
	s.InventoryCh <- payload
	writeJSON(w, protocol.AckResponse{Ok: true})
}

// handleStateReport procesa POST /state-report: una réplica paralela reporta
// su inventario y vetos finales (tras ejecutar sus instrucciones) para la
// fase de comparación por quórum.
// Entrada/Salida HTTP: JSON StatePayload -> JSON StateResponse.
func (s *Server) handleStateReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload protocol.StatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "json inválido", http.StatusBadRequest)
		return
	}
	s.StateCh <- payload
	writeJSON(w, protocol.StateResponse{Received: true})
}

// handleHealth responde 200 OK simple, usado para detectar si una réplica
// está disponible antes de intentar enviarle datos.
// Entrada/Salida HTTP: ninguna -> JSON AckResponse.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, protocol.AckResponse{Ok: true})
}

// writeJSON serializa y escribe una respuesta JSON con status 200.
// Entrada: ResponseWriter, valor a serializar. Salida: ninguna.
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// PeerKey identifica a un proceso del sistema por su máquina y su ID.
type PeerKey struct {
	MachineID int
	ProcessID int
}

// String representa la clave en formato "M<m>P<p>", útil para logs.
// Entrada: ninguna. Salida: string.
func (k PeerKey) String() string {
	return fmt.Sprintf("M%dP%d", k.MachineID, k.ProcessID)
}

// PeerSnapshot es la copia de solo lectura que este proceso guarda del
// inventario y vetos de OTRO proceso del sistema, recibida vía PushInventory.
// Seq es un número de secuencia monotónico (Unix nanosegundos al momento del
// broadcast en el proceso emisor): permite descartar actualizaciones que
// lleguen fuera de orden por la red, garantizando que un peer nunca
// sobreescriba un estado reciente con uno stale.
type PeerSnapshot struct {
	Inventory []Item      `json:"inventory"`
	Vetos     []VetoEntry `json:"vetos"`
	Seq       int64       `json:"seq"`
}

// PeerTable mantiene, para este proceso, una copia del inventario y vetos
// de TODOS los demás procesos del sistema. Es thread-safe y persiste cada
// entrada en un archivo JSON dentro de logs/ para sobrevivir reinicios.
// El archivo de cada proceso externo MxPy se guarda como:
//
//	logs/peer_M<ownerM>P<ownerP>_copia_M<xM>P<yP>.json
//
// donde ownerM/ownerP es este proceso (el dueño de la tabla).
type PeerTable struct {
	mu     sync.RWMutex
	data   map[PeerKey]PeerSnapshot
	ownerM int // MachineID de este proceso
	ownerP int // ProcessID de este proceso
}

// NewPeerTable crea una tabla de peers vacía para el proceso ownerM/ownerP.
// Intenta cargar desde disco cualquier entrada persistida previamente.
// Entrada: ownerM, ownerP. Salida: *PeerTable inicializada.
func NewPeerTable(ownerM, ownerP int) *PeerTable {
	t := &PeerTable{
		data:   make(map[PeerKey]PeerSnapshot),
		ownerM: ownerM,
		ownerP: ownerP,
	}
	t.loadFromDisk()
	return t
}

// peerFilePath retorna la ruta del archivo JSON donde se persiste la copia
// del proceso peerKey para este owner.
// Entrada: PeerKey del peer. Salida: path string.
func (t *PeerTable) peerFilePath(key PeerKey) string {
	return fmt.Sprintf("logs/peer_M%dP%d_copia_M%dP%d.json",
		t.ownerM, t.ownerP, key.MachineID, key.ProcessID)
}

// Update guarda o reemplaza la copia conocida de un proceso específico,
// tanto en memoria como en disco, SOLO si seq es mayor al seq ya almacenado.
// Esto descarta actualizaciones stale que llegan fuera de orden por la red
// (e.g. reintentos de broadcasts anteriores que llegan tarde).
// Entrada: PeerKey del emisor, inventario, vetos, número de secuencia.
// Salida: ninguna.
func (t *PeerTable) Update(key PeerKey, inventory []Item, vetos []VetoEntry, seq int64) {
	invCp := make([]Item, len(inventory))
	copy(invCp, inventory)
	vetoCp := make([]VetoEntry, len(vetos))
	copy(vetoCp, vetos)
	snap := PeerSnapshot{Inventory: invCp, Vetos: vetoCp, Seq: seq}

	t.mu.Lock()
	existing, exists := t.data[key]
	if exists && existing.Seq >= seq {
		// Llegó un mensaje más antiguo que el estado actual: ignorar.
		t.mu.Unlock()
		return
	}
	t.data[key] = snap
	t.mu.Unlock()

	// Sincrónico: garantiza que la copia quede en disco antes de retornar,
	// para que sobreviva si el proceso es matado justo después del push.
	t.saveToDisk(key, snap)
}

// saveToDisk escribe la snapshot de un peer a su archivo JSON.
// Entrada: PeerKey, PeerSnapshot. Salida: ninguna (loguea si falla).
func (t *PeerTable) saveToDisk(key PeerKey, snap PeerSnapshot) {
	_ = os.MkdirAll("logs", 0755)
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(t.peerFilePath(key), data, 0644)
}

// loadFromDisk intenta cargar desde disco todas las copias persistidas
// previamente para este owner. Se llama solo en NewPeerTable.
// Entrada: ninguna. Salida: ninguna.
func (t *PeerTable) loadFromDisk() {
	pattern := fmt.Sprintf("logs/peer_M%dP%d_copia_*.json", t.ownerM, t.ownerP)
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		return
	}
	for _, path := range matches {
		var key PeerKey
		_, err := fmt.Sscanf(
			filepath.Base(path),
			"peer_M%dP%d_copia_M%dP%d.json",
			new(int), new(int), &key.MachineID, &key.ProcessID,
		)
		if err != nil {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var snap PeerSnapshot
		if json.Unmarshal(data, &snap) == nil {
			t.data[key] = snap
		}
	}
}

// Get devuelve la copia almacenada de un proceso específico, si existe.
// Entrada: PeerKey del proceso consultado. Salida: snapshot y si fue encontrado.
func (t *PeerTable) Get(key PeerKey) (PeerSnapshot, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	snap, ok := t.data[key]
	return snap, ok
}

// Snapshot devuelve una copia de todas las entradas actuales de la tabla.
// Entrada: ninguna. Salida: mapa copia de PeerKey -> PeerSnapshot.
func (t *PeerTable) Snapshot() map[PeerKey]PeerSnapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[PeerKey]PeerSnapshot, len(t.data))
	for k, v := range t.data {
		out[k] = v
	}
	return out
}

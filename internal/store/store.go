package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// Item representa un producto del inventario con su cantidad disponible.
type Item struct {
	Nombre   string `json:"nombre"`
	Cantidad int    `json:"cantidad"`
}

// VetoEntry representa un veto activo sobre una persona, con el counter de
// instrucciones restantes antes de ser perdonada automáticamente.
type VetoEntry struct {
	Persona string `json:"persona"`
	Counter int    `json:"counter"`
}

// Store es la "base de datos" local de un proceso: su PROPIO inventario y
// vetos, persistidos como archivo JSON en disco. No incluye las copias de
// los demás procesos (eso vive en process.PeerTable).
// Es thread-safe mediante un RWMutex.
type Store struct {
	mu            sync.RWMutex
	inventory     []Item
	vetos         map[string]int
	inventoryPath string
}

// New crea un Store vacío asociado a un archivo de inventario propio.
// Entrada: ruta donde se persistirá el inventario propio de este proceso.
// Salida: *Store inicializado.
func New(inventoryPath string) *Store {
	return &Store{
		vetos:         make(map[string]int),
		inventoryPath: inventoryPath,
	}
}

// LoadFromTemplate copia el contenido de un archivo de inventario plantilla
// hacia el archivo propio de este proceso, y lo carga en memoria.
// Entrada: ruta del archivo plantilla elegido aleatoriamente.
// Salida: error si no se puede leer la plantilla o escribir la copia.
func (s *Store) LoadFromTemplate(templatePath string) error {
	data, err := os.ReadFile(templatePath)
	if err != nil {
		return fmt.Errorf("no se pudo leer plantilla %s: %w", templatePath, err)
	}
	var items []Item
	if err := json.Unmarshal(data, &items); err != nil {
		return fmt.Errorf("plantilla %s inválida: %w", templatePath, err)
	}

	s.mu.Lock()
	s.inventory = items
	s.mu.Unlock()

	return s.persist()
}

// persist escribe el inventario actual en el archivo propio del proceso.
// Entrada: ninguna. Salida: error si falla la escritura.
func (s *Store) persist() error {
	s.mu.RLock()
	data, err := json.MarshalIndent(s.inventory, "", "  ")
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	return os.WriteFile(s.inventoryPath, data, 0644)
}

// SetInventory reemplaza el inventario completo (usado tras una
// recuperación exitosa por quorum).
// Entrada: slice de Items. Salida: ninguna (persiste a disco internamente).
func (s *Store) SetInventory(items []Item) {
	s.mu.Lock()
	cp := make([]Item, len(items))
	copy(cp, items)
	s.inventory = cp
	s.mu.Unlock()
	_ = s.persist()
}

// SetVetos reemplaza la lista de vetos completa (usado tras una
// recuperación exitosa por quorum).
// Entrada: slice de VetoEntry. Salida: ninguna.
func (s *Store) SetVetos(entries []VetoEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vetos = make(map[string]int, len(entries))
	for _, e := range entries {
		s.vetos[e.Persona] = e.Counter
	}
}

// GetInventory devuelve una copia del inventario actual.
// Entrada: ninguna. Salida: slice de Items.
func (s *Store) GetInventory() []Item {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make([]Item, len(s.inventory))
	copy(cp, s.inventory)
	return cp
}

// Buy intenta descontar `cantidad` unidades de `producto` para `persona`.
// Entrada: persona, producto, cantidad. Salida: "VALIDO", "DENEGADO" (vetado)
// o "NO VALIDO" (sin stock o producto inexistente).
func (s *Store) Buy(persona, producto string, cantidad int) string {
	s.mu.Lock()
	if _, vetado := s.vetos[persona]; vetado {
		s.mu.Unlock()
		return "DENEGADO"
	}
	result := "NO VALIDO"
	for i, item := range s.inventory {
		if item.Nombre == producto {
			if item.Cantidad >= cantidad {
				s.inventory[i].Cantidad -= cantidad
				result = "VALIDO"
			}
			break
		}
	}
	s.mu.Unlock()
	if result == "VALIDO" {
		_ = s.persist()
	}
	return result
}

// Veto agrega o reinicia el veto sobre una persona, fijando su counter a 5.
// Entrada: nombre de persona. Salida: counter resultante (siempre 5).
func (s *Store) Veto(persona string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.vetos[persona] = 5
	return 5
}

// Pardon elimina el veto de una persona, sin importar su counter actual.
// Entrada: nombre de persona. Salida: ninguna.
func (s *Store) Pardon(persona string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.vetos, persona)
}

// DecrementVetos reduce en 1 el counter de todos los vetos activos, y
// perdona (elimina) automáticamente a quienes lleguen a 0.
// Entrada: ninguna.
// Salida: slice de personas perdonadas automáticamente, y un bool que indica
// si hubo cualquier cambio en los counters (incluso sin llegar a 0). Este
// segundo valor permite al caller saber si debe hacer broadcast aunque nadie
// haya sido perdonado, evitando divergencia de estado entre procesos.
func (s *Store) DecrementVetos() (pardoned []string, anyChanged bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p, c := range s.vetos {
		c--
		anyChanged = true // hubo al menos un decremento: el estado cambió
		if c <= 0 {
			delete(s.vetos, p)
			pardoned = append(pardoned, p)
		} else {
			s.vetos[p] = c
		}
	}
	return
}

// GetVetos devuelve la lista de vetos activos como slice.
// Entrada: ninguna. Salida: slice de VetoEntry.
func (s *Store) GetVetos() []VetoEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]VetoEntry, 0, len(s.vetos))
	for p, c := range s.vetos {
		out = append(out, VetoEntry{Persona: p, Counter: c})
	}
	return out
}

// IsVetoed informa si una persona está vetada actualmente.
// Entrada: nombre de persona. Salida: bool.
func (s *Store) IsVetoed(persona string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.vetos[persona]
	return ok
}

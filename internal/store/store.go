package store

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"tarea3/internal/protocol"
)

// Store representa la "base de datos" local de un proceso: su inventario y
// su lista de vetos, persistidos como archivos JSON en disco (cada proceso
// tiene su propio archivo, nunca comparte el original de /inventario).
// Es thread-safe mediante un RWMutex, ya que se accede tanto desde la
// ejecución de instrucciones como desde el servidor HTTP.
type Store struct {
	mu            sync.RWMutex
	inventory     []protocol.Item
	vetos         map[string]int // persona -> counter restante
	inventoryPath string         // ruta del archivo JSON propio de este proceso
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
// (de la carpeta /inventario) hacia el archivo propio de este proceso, y lo
// carga en memoria. Esto asegura que el proceso nunca modifica el archivo
// original, solo su copia individual.
// Entrada: ruta del archivo plantilla elegido aleatoriamente.
// Salida: error si no se puede leer la plantilla o escribir la copia.
func (s *Store) LoadFromTemplate(templatePath string) error {
	data, err := os.ReadFile(templatePath)
	if err != nil {
		return fmt.Errorf("no se pudo leer plantilla %s: %w", templatePath, err)
	}
	var items []protocol.Item
	if err := json.Unmarshal(data, &items); err != nil {
		return fmt.Errorf("plantilla %s inválida: %w", templatePath, err)
	}

	s.mu.Lock()
	s.inventory = items
	s.mu.Unlock()

	return s.persist()
}

// persist escribe el inventario actual en el archivo propio del proceso en
// disco (la "copia" individual mencionada en el enunciado).
// Entrada: ninguna (usa s.inventoryPath). Salida: error si falla la escritura.
func (s *Store) persist() error {
	s.mu.RLock()
	data, err := json.MarshalIndent(s.inventory, "", "  ")
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	return os.WriteFile(s.inventoryPath, data, 0644)
}

// SetInventory reemplaza el inventario completo (usado al recibir el
// inventario inicial de una réplica paralela, o tras una recuperación).
// Entrada: slice de Items. Salida: ninguna (persiste a disco internamente).
func (s *Store) SetInventory(items []protocol.Item) {
	s.mu.Lock()
	cp := make([]protocol.Item, len(items))
	copy(cp, items)
	s.inventory = cp
	s.mu.Unlock()
	_ = s.persist()
}

// GetInventory devuelve una copia del inventario actual.
// Entrada: ninguna. Salida: slice de Items.
func (s *Store) GetInventory() []protocol.Item {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make([]protocol.Item, len(s.inventory))
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
// Entrada: ninguna. Salida: slice de personas perdonadas automáticamente.
func (s *Store) DecrementVetos() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var pardoned []string
	for p, c := range s.vetos {
		c--
		if c <= 0 {
			delete(s.vetos, p)
			pardoned = append(pardoned, p)
		} else {
			s.vetos[p] = c
		}
	}
	return pardoned
}

// GetVetos devuelve la lista de vetos activos como slice ordenable.
// Entrada: ninguna. Salida: slice de VetoEntry.
func (s *Store) GetVetos() []protocol.VetoEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]protocol.VetoEntry, 0, len(s.vetos))
	for p, c := range s.vetos {
		out = append(out, protocol.VetoEntry{Persona: p, Counter: c})
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

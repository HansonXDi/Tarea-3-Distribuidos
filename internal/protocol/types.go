package protocol

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

// InventoryPayload es el cuerpo enviado en POST /inventory cuando un proceso
// recién levantado distribuye su inventario inicial a sus réplicas paralelas.
type InventoryPayload struct {
	MachineID int    `json:"machine_id"`
	ProcessID int    `json:"process_id"`
	Inventory []Item `json:"inventory"`
}

// StatePayload es el cuerpo enviado en POST /state-report durante la fase de
// comparación final: cada réplica reporta su inventario y vetos resultantes
// tras ejecutar sus instrucciones, para que el coordinador determine el
// quórum 2/3.
type StatePayload struct {
	MachineID int         `json:"machine_id"`
	ProcessID int         `json:"process_id"`
	Inventory []Item      `json:"inventory"`
	Vetos     []VetoEntry `json:"vetos"`
	Infected  bool        `json:"infected"`
}

// StateResponse es la respuesta a un StatePayload; permite al solicitante
// saber si su reporte fue aceptado.
type StateResponse struct {
	Received bool `json:"received"`
}

// AckResponse es una respuesta genérica de confirmación simple.
type AckResponse struct {
	Ok bool `json:"ok"`
}

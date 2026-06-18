package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"tarea3/internal/protocol"
	"time"
)

// Client encapsula las llamadas REST hacia otro proceso (réplica paralela).
type Client struct {
	BaseURL string // ej: http://10.10.28.36:8101
	http    *http.Client
}

// NewClient crea un cliente HTTP apuntando a la URL base de un peer.
// Entrada: baseURL (incluye esquema y puerto). Salida: *Client.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: baseURL,
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

// WaitHealthy reintenta GET /health hasta que responda OK o se agoten los
// intentos. Se usa al iniciar, para esperar a que las réplicas paralelas
// estén listas antes de enviarles el inventario inicial.
// Entrada: intentos máximos, espera entre intentos. Salida: true si respondió OK.
func (c *Client) WaitHealthy(maxAttempts int, delay time.Duration) bool {
	for i := 0; i < maxAttempts; i++ {
		resp, err := c.http.Get(c.BaseURL + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(delay)
	}
	return false
}

// SendInventory envía POST /inventory con el inventario inicial de este
// proceso, para que la réplica paralela lo registre.
// Entrada: payload con machineID, processID e inventario. Salida: error de red/HTTP.
func (c *Client) SendInventory(payload protocol.InventoryPayload) error {
	return c.postJSON("/inventory", payload, &protocol.AckResponse{})
}

// SendStateReport envía POST /state-report con el inventario y vetos finales
// de este proceso, usado en la fase de comparación por quórum.
// Entrada: payload con el estado final. Salida: error de red/HTTP.
func (c *Client) SendStateReport(payload protocol.StatePayload) error {
	return c.postJSON("/state-report", payload, &protocol.StateResponse{})
}

// postJSON serializa el body, hace POST, y decodifica la respuesta en out.
// Entrada: path, body a enviar, puntero donde decodificar la respuesta.
// Salida: error si falla la conexión, el status, o la decodificación.
func (c *Client) postJSON(path string, body interface{}, out interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	resp, err := c.http.Post(c.BaseURL+path, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d desde %s%s", resp.StatusCode, c.BaseURL, path)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

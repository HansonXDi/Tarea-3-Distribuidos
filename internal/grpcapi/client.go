package grpcapi

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client encapsula la conexión gRPC hacia otro proceso del sistema.
type Client struct {
	Addr string // ej: 10.10.28.36:8101
	conn *grpc.ClientConn
	rpc  ExpendedoraServiceClient
}

// NewClient crea (y conecta de forma perezosa) un cliente gRPC hacia la
// dirección indicada. La conexión real ocurre en el primer RPC.
// Entrada: addr (host:puerto, sin esquema). Salida: *Client, error si la
// configuración de la conexión falla inmediatamente.
func NewClient(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		Addr: addr,
		conn: conn,
		rpc:  NewExpendedoraServiceClient(conn),
	}, nil
}

// Close cierra la conexión gRPC subyacente.
// Entrada: ninguna. Salida: error si falla el cierre.
func (c *Client) Close() error {
	return c.conn.Close()
}

// WaitHealthy reintenta el RPC Health hasta que responda OK o se agoten los
// intentos. Se usa al iniciar, para esperar a que otros procesos estén listos.
// Entrada: intentos máximos, espera entre intentos. Salida: true si respondió OK.
func (c *Client) WaitHealthy(maxAttempts int, delay time.Duration) bool {
	for i := 0; i < maxAttempts; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		reply, err := c.rpc.Health(ctx, &HealthRequest{})
		cancel()
		if err == nil && reply != nil && reply.Ok {
			return true
		}
		time.Sleep(delay)
	}
	return false
}

// PushInventory envía este inventario y vetos al proceso remoto, vía el RPC
// PushInventory, para que actualice su copia de solo lectura de este proceso.
// Entrada: machineID/processID propios, inventario, vetos. Salida: error de RPC.
func (c *Client) PushInventory(machineID, processID int, inventory []*Item, vetos []*VetoEntry, seq int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := c.rpc.PushInventory(ctx, &InventoryUpdate{
		MachineId: int32(machineID),
		ProcessId: int32(processID),
		Inventory: inventory,
		Vetos:     vetos,
		Seq:       seq,
	})
	return err
}

// QueryInventory solicita al proceso remoto su copia almacenada del
// inventario y vetos de un proceso específico (usado en recuperación).
// Entrada: machineID/processID del proceso objetivo a consultar.
// Salida: found, inventario, vetos, error de RPC.
func (c *Client) QueryInventory(targetMachineID, targetProcessID int) (bool, []*Item, []*VetoEntry, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	reply, err := c.rpc.QueryInventory(ctx, &InventoryQuery{
		TargetMachineId: int32(targetMachineID),
		TargetProcessId: int32(targetProcessID),
	})
	if err != nil {
		return false, nil, nil, err
	}
	return reply.Found, reply.Inventory, reply.Vetos, nil
}

package grpcapi

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
)

// PushHandler procesa una actualización de inventario recibida de otro
// proceso. Se inyecta desde el paquete process para mantener esta capa
// enfocada solo en transporte gRPC.
type PushHandler func(machineID, processID int, inventory []*Item, vetos []*VetoEntry, seq int64)

// QueryHandler responde una consulta sobre el inventario de un proceso
// específico. Se inyecta desde el paquete process.
type QueryHandler func(targetMachineID, targetProcessID int) (found bool, inventory []*Item, vetos []*VetoEntry)

// Server implementa ExpendedoraServiceServer (interfaz generada por
// protoc-gen-go-grpc a partir de proto/expendedora.proto).
type Server struct {
	UnimplementedExpendedoraServiceServer

	Port int

	OnPush  PushHandler
	OnQuery QueryHandler

	grpcServer *grpc.Server
}

// NewServer crea un servidor gRPC para el puerto indicado. Los handlers
// OnPush y OnQuery deben asignarse antes de llamar Start.
// Entrada: puerto de escucha. Salida: *Server listo para configurar e iniciar.
func NewServer(port int) *Server {
	return &Server{Port: port}
}

// Start registra el servicio y arranca el servidor gRPC en una goroutine.
// Entrada: ninguna. Salida: ninguna (loguea fatal si no puede escuchar).
func (s *Server) Start() {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
	if err != nil {
		log.Fatalf("[gRPC] No se pudo escuchar en :%d: %v", s.Port, err)
	}

	s.grpcServer = grpc.NewServer()
	RegisterExpendedoraServiceServer(s.grpcServer, s)

	go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Fatalf("[gRPC] Error sirviendo en :%d: %v", s.Port, err)
		}
	}()
}

// Health implementa el RPC Health: confirma disponibilidad simple.
// Entrada: contexto, HealthRequest vacío. Salida: HealthReply{Ok: true}.
func (s *Server) Health(ctx context.Context, req *HealthRequest) (*HealthReply, error) {
	return &HealthReply{Ok: true}, nil
}

// PushInventory implementa el RPC PushInventory: delega al handler de
// negocio inyectado (OnPush), que actualiza la PeerTable correspondiente.
// Entrada: contexto, InventoryUpdate con el inventario del emisor.
// Salida: Ack{Ok: true}.
func (s *Server) PushInventory(ctx context.Context, req *InventoryUpdate) (*Ack, error) {
	if s.OnPush != nil {
		s.OnPush(int(req.MachineId), int(req.ProcessId), req.Inventory, req.Vetos, req.Seq)
	}
	return &Ack{Ok: true}, nil
}

// QueryInventory implementa el RPC QueryInventory: delega al handler de
// negocio inyectado (OnQuery), que responde con la copia almacenada del
// proceso consultado (o datos falsos si esta máquina está infectada).
// Entrada: contexto, InventoryQuery con el proceso objetivo.
// Salida: InventoryQueryReply con la copia encontrada (o found=false).
func (s *Server) QueryInventory(ctx context.Context, req *InventoryQuery) (*InventoryQueryReply, error) {
	if s.OnQuery == nil {
		return &InventoryQueryReply{Found: false}, nil
	}
	found, inv, vetos := s.OnQuery(int(req.TargetMachineId), int(req.TargetProcessId))
	return &InventoryQueryReply{Found: found, Inventory: inv, Vetos: vetos}, nil
}

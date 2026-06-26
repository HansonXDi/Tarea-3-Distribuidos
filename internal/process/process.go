package process

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"tarea3/internal/grpcapi"
	"tarea3/internal/store"
)

// PeerAddr identifica la dirección de red de OTRO proceso del sistema
// (puede estar en la misma máquina o en otra), usado para hacer broadcast
// de inventario y para consultas de recuperación.
type PeerAddr struct {
	MachineID int
	ProcessID int
	Addr      string // host:puerto, ej "10.10.28.36:8101"
}

// Process representa una expendedora: tiene su propio Store (inventario y
// vetos persistidos en disco), una PeerTable con copias de solo lectura de
// TODOS los demás procesos del sistema, y se comunica vía gRPC.
type Process struct {
	MachineID    int
	ProcessID    int
	instructFile string

	st        *store.Store
	peerTable *store.PeerTable
	server    *grpcapi.Server
	allPeers  []PeerAddr // todos los demás procesos del sistema (cualquier máquina)

	mu       sync.Mutex
	infected bool

	logPath     string
	vetoLogPath string
}

// New crea un Process con su Store, PeerTable y servidor gRPC propios.
// Entrada: machineID, processID, archivo de instrucciones, todos los demás
// procesos del sistema, puerto de escucha propio.
// Salida: *Process listo para Run() o Recover().
func New(machineID, processID int, instructFile string, allPeers []PeerAddr, port int) *Process {
	invPath := fmt.Sprintf("logs/inventario_propio_M%dP%d.json", machineID, processID)
	p := &Process{
		MachineID:    machineID,
		ProcessID:    processID,
		instructFile: instructFile,
		st:           store.New(invPath),
		peerTable:    store.NewPeerTable(machineID, processID),
		server:       grpcapi.NewServer(port),
		allPeers:     allPeers,
		logPath:      fmt.Sprintf("logs/inventario_M%dP%d.log", machineID, processID),
		vetoLogPath:  fmt.Sprintf("logs/vetos_M%dP%d.log", machineID, processID),
	}

	p.server.OnPush = p.handlePush
	p.server.OnQuery = p.handleQuery

	return p
}

// Run ejecuta el ciclo de vida normal de un proceso nuevo (no recuperado):
//  1. Levanta el servidor gRPC propio.
//  2. Revisa el flag de infección de la máquina.
//  3. Sortea un inventario plantilla al azar y lo copia como propio.
//  4. Hace broadcast de su inventario inicial a TODOS los demás procesos
//     del sistema (no solo a un grupo de réplica, ya que ese concepto no
//     existe en este modelo).
//  5. Ejecuta sus instrucciones locales; cada vez que una instrucción
//     modifica su inventario, vuelve a hacer broadcast del nuevo estado.
//  6. Queda corriendo indefinidamente, respondiendo a Health/PushInventory/
//     QueryInventory de otros procesos.
//
// Entrada: ninguna. Salida: ninguna (bloquea).
func (p *Process) Run() {
	p.server.Start()

	if p.infectionFlagExists() {
		p.SetInfected(true)
	}

	if err := p.pickAndLoadInventory(); err != nil {
		log.Fatalf("[P%d] No se pudo cargar inventario: %v", p.ProcessID, err)
	}

	p.waitForPeers()
	p.broadcastInventory()

	log.Printf("[P%d] Ejecutando instrucciones...", p.ProcessID)
	p.runInstructions()
	log.Printf("[P%d] Instrucciones finalizadas. Proceso activo, escuchando peticiones.", p.ProcessID)
}

// pickAndLoadInventory elige aleatoriamente un archivo de /inventario y lo
// copia como el inventario propio de este proceso.
// Entrada: ninguna. Salida: error si no hay plantillas disponibles.
func (p *Process) pickAndLoadInventory() error {
	matches, err := filepath.Glob("inventario/*.json")
	if err != nil || len(matches) == 0 {
		return fmt.Errorf("no hay archivos de inventario en /inventario")
	}
	chosen := matches[rand.Intn(len(matches))]
	log.Printf("[P%d] Plantilla de inventario elegida: %s", p.ProcessID, chosen)
	return p.st.LoadFromTemplate(chosen)
}

// waitForPeers espera hasta 2 segundos a que cada uno de los demás procesos
// del sistema responda su RPC Health, antes de continuar.
// Entrada: ninguna. Salida: ninguna (continúa de todos modos tras el timeout).
func (p *Process) waitForPeers() {
	var wg sync.WaitGroup
	for _, peer := range p.allPeers {
		wg.Add(1)
		go func(peer PeerAddr) {
			defer wg.Done()
			c, err := grpcapi.NewClient(peer.Addr)
			if err != nil {
				log.Printf("[P%d] No se pudo preparar conexión a M%dP%d: %v", p.ProcessID, peer.MachineID, peer.ProcessID, err)
				return
			}
			defer c.Close()
			ok := c.WaitHealthy(20, 100*time.Millisecond) // 20*100ms = 2s máx
			if !ok {
				log.Printf("[P%d] Advertencia: M%dP%d (%s) no respondió a tiempo", p.ProcessID, peer.MachineID, peer.ProcessID, peer.Addr)
			}
		}(peer)
	}
	wg.Wait()
	log.Printf("[P%d] Espera de procesos del sistema finalizada.", p.ProcessID)
}

// broadcastInventory envía el inventario y vetos actuales de este proceso a
// TODOS los demás procesos del sistema vía PushInventory, con reintentos
// por si algún peer no estaba listo al primer intento.
// Entrada: ninguna. Salida: ninguna (loguea errores, no es fatal).
func (p *Process) broadcastInventory() {
	// El snapshot y el seq se capturan ANTES de lanzar las goroutines, para
	// que todos los peers reciban exactamente el mismo estado y el mismo número
	// de secuencia, sin importar cuándo ejecute cada goroutine. Así, si un
	// reintento llega tarde a algún peer, el receptor lo descarta porque su
	// seq ya es mayor (guardado de un broadcast posterior).
	invItems := toProtoItems(p.st.GetInventory())
	vetoItems := toProtoVetos(p.st.GetVetos())
	seq := time.Now().UnixNano()

	for _, peer := range p.allPeers {
		go func(peer PeerAddr) {
			c, err := grpcapi.NewClient(peer.Addr)
			if err != nil {
				log.Printf("[P%d] Error preparando conexión a M%dP%d: %v", p.ProcessID, peer.MachineID, peer.ProcessID, err)
				return
			}
			defer c.Close()

			for attempt := 1; attempt <= 10; attempt++ {
				err := c.PushInventory(p.MachineID, p.ProcessID, invItems, vetoItems, seq)
				if err == nil {
					return
				}
				time.Sleep(300 * time.Millisecond)
			}
			log.Printf("[P%d] ERROR: no se pudo enviar inventario a M%dP%d tras varios intentos.", p.ProcessID, peer.MachineID, peer.ProcessID)
		}(peer)
	}
}

// handlePush procesa una actualización de inventario recibida de OTRO
// proceso (vía RPC PushInventory) y la guarda en la PeerTable de este
// proceso. Si esta máquina está infectada y alguien le hace push, igual se
// almacena fielmente lo recibido (la infección afecta lo que ESTA máquina
// RESPONDE/ENVÍA, no lo que registra de otros).
// Entrada: machineID/processID del emisor, su inventario y vetos.
// Salida: ninguna.
func (p *Process) handlePush(machineID, processID int, inventory []*grpcapi.Item, vetos []*grpcapi.VetoEntry, seq int64) {
	key := store.PeerKey{MachineID: machineID, ProcessID: processID}
	p.peerTable.Update(key, fromProtoItems(inventory), fromProtoVetos(vetos), seq)
}

// handleQuery responde una consulta sobre el inventario de un proceso
// específico. Si el proceso consultado es ESTE MISMO proceso, responde con
// su propio estado real (o falso si está infectado). Si es OTRO proceso,
// responde con la copia que tiene almacenada en su PeerTable (o falsa, si
// esta máquina está infectada, sin importar de qué proceso se trate).
// Entrada: machineID/processID del proceso consultado.
// Salida: found, inventario, vetos (reales o falsos según infección).
func (p *Process) handleQuery(targetMachineID, targetProcessID int) (bool, []*grpcapi.Item, []*grpcapi.VetoEntry) {
	if p.IsInfected() {
		return true, fakeItems(), fakeVetos()
	}

	if targetMachineID == p.MachineID && targetProcessID == p.ProcessID {
		return true, toProtoItems(p.st.GetInventory()), toProtoVetos(p.st.GetVetos())
	}

	key := store.PeerKey{MachineID: targetMachineID, ProcessID: targetProcessID}
	snap, ok := p.peerTable.Get(key)
	if !ok {
		return false, nil, nil
	}
	return true, toProtoItems(snap.Inventory), toProtoVetos(snap.Vetos)
}

// runInstructions lee y ejecuta cada línea del archivo de instrucciones
// asignado a este proceso, generando una entrada de log por cada una y
// haciendo broadcast del inventario tras cada cambio. Trunca los logs
// propios al inicio para que cada nueva ejecución sobreescriba la anterior.
// Entrada: ninguna. Salida: ninguna.
func (p *Process) runInstructions() {
	os.MkdirAll("logs", 0755)
	os.Truncate(p.logPath, 0)
	os.Truncate(p.vetoLogPath, 0)

	f, err := os.Open(p.instructFile)
	if err != nil {
		log.Printf("[P%d] No se pudo abrir %s: %v", p.ProcessID, p.instructFile, err)
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		result, changed := p.execInstruction(line)
		p.appendLog(line, result)
		if changed {
			p.broadcastInventory()
		}
	}
	p.writeVetoLog()

	// Broadcast final al terminar todas las instrucciones: garantiza que
	// todos los peers tengan el estado definitivo y consistente del proceso,
	// independientemente de si algún broadcast intermedio llegó en mal momento.
	p.broadcastInventory()
	log.Printf("[P%d] Broadcast final de inventario y vetos completado.", p.ProcessID)
}

// execInstruction parsea y ejecuta una instrucción de texto (VETAR,
// COMPRAR o PERDONAR) sobre el Store local de este proceso.
// Entrada: línea de instrucción. Salida: resultado a loguear, y si la
// instrucción modificó el inventario o vetos (para decidir si hacer
// broadcast).
func (p *Process) execInstruction(line string) (result string, changed bool) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "", false
	}
	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "VETAR":
		if len(parts) < 2 {
			return "", false
		}
		persona := strings.Join(parts[1:], " ")
		p.st.Veto(persona)
		return "", true

	case "COMPRAR":
		if len(parts) < 4 {
			return "NO VALIDO", false
		}
		cantidad := 0
		fmt.Sscanf(parts[len(parts)-1], "%d", &cantidad)
		producto := parts[len(parts)-2]
		persona := strings.Join(parts[1:len(parts)-2], " ")
		res := p.st.Buy(persona, producto, cantidad)
		// DecrementVetos ahora devuelve (pardoned, anyChanged). anyChanged es
		// true si había al menos un veto activo (aunque nadie llegara a 0),
		// garantizando que el broadcast ocurra siempre que el estado de vetos
		// se haya modificado, evitando divergencia de counters entre procesos.
		_, vetoChanged := p.st.DecrementVetos()
		didChange := res == "VALIDO" || vetoChanged
		return res, didChange

	case "PERDONAR":
		if len(parts) < 2 {
			return "", false
		}
		persona := strings.Join(parts[1:], " ")
		p.st.Pardon(persona)
		return "", true
	}
	return "", false
}

// Recover ejecuta el protocolo de recuperación de un proceso caído: NO
// re-ejecuta instrucciones. En su lugar, consulta a TODOS los demás
// procesos del sistema (vía QueryInventory) qué copia tienen almacenada de
// este proceso, y reconstruye su inventario y vetos eligiendo la respuesta
// que más se repita, siempre que alcance un quorum de al menos 2/3 del
// total de respuestas posibles. Si no se alcanza el quorum, el proceso NO
// puede recuperarse y se reporta el error de integridad.
// Entrada: ninguna. Salida: ninguna (bloquea sirviendo tras recuperar, o
// termina con log.Fatal si no se alcanza el quorum).
func (p *Process) Recover() {
	p.server.Start()

	if p.infectionFlagExists() {
		p.SetInfected(true)
	}

	log.Printf("[P%d] Iniciando recuperación: consultando a todos los procesos...", p.ProcessID)

	var mu sync.Mutex
	var replies []quorumReply
	var wg sync.WaitGroup

	for _, peer := range p.allPeers {
		wg.Add(1)
		go func(peer PeerAddr) {
			defer wg.Done()
			c, err := grpcapi.NewClient(peer.Addr)
			if err != nil {
				log.Printf("[P%d] No se pudo conectar a M%dP%d para recuperación: %v", p.ProcessID, peer.MachineID, peer.ProcessID, err)
				return
			}
			defer c.Close()

			found, inv, vetos, err := c.QueryInventory(p.MachineID, p.ProcessID)
			if err != nil || !found {
				log.Printf("[P%d] M%dP%d no tiene copia o no respondió: %v", p.ProcessID, peer.MachineID, peer.ProcessID, err)
				return
			}
			mu.Lock()
			replies = append(replies, quorumReply{inv: fromProtoItems(inv), vetos: fromProtoVetos(vetos)})
			mu.Unlock()
		}(peer)
	}
	wg.Wait()

	if len(replies) == 0 {
		log.Fatalf("[P%d] RECUPERACIÓN FALLIDA: ningún proceso respondió. Todas las máquinas han sido infectadas, por favor revíseme.", p.ProcessID)
	}

	bestInv, bestVetos, bestCount := majorityState(replies)
	// El total de procesos consultables es len(allPeers): este proceso no
	// puede consultarse a sí mismo (acaba de reiniciarse sin inventario),
	// así que el máximo posible de respuestas es exactamente len(allPeers).
	total := len(p.allPeers)
	threshold := float64(total) * 2.0 / 3.0

	if float64(bestCount) < threshold {
		log.Fatalf(
			"[P%d] RECUPERACIÓN FALLIDA: quorum insuficiente (%d/%d, se requiere >= 2/3). "+
				"Todas las máquinas han sido infectadas, por favor revíseme.",
			p.ProcessID, bestCount, total,
		)
	}

	p.st.SetInventory(bestInv)
	p.st.SetVetos(bestVetos)
	p.writeVetoLog()

	log.Printf("[P%d] RECUPERACIÓN EXITOSA (%d/%d). Inventario y vetos reconstruidos por quorum.", p.ProcessID, bestCount, total)

	p.broadcastInventory()
	log.Printf("[P%d] Proceso recuperado, activo y escuchando peticiones.", p.ProcessID)
}

// quorumReply es una respuesta recolectada de un peer durante Recover.
type quorumReply struct {
	inv   []store.Item
	vetos []store.VetoEntry
}

// majorityState compara las respuestas recolectadas y determina cuál
// combinación de inventario+vetos se repite más veces.
// Entrada: slice de respuestas (inventario, vetos) de los peers consultados.
// Salida: inventario ganador, vetos ganadores, cantidad de votos que obtuvo.
func majorityState(replies []quorumReply) ([]store.Item, []store.VetoEntry, int) {
	counts := make(map[string]int)
	invByKey := make(map[string][]store.Item)
	vetoByKey := make(map[string][]store.VetoEntry)

	for _, r := range replies {
		key := stateKeyOf(r.inv, r.vetos)
		counts[key]++
		invByKey[key] = r.inv
		vetoByKey[key] = r.vetos
	}

	var bestKey string
	best := 0
	for k, c := range counts {
		if c > best {
			best = c
			bestKey = k
		}
	}
	return invByKey[bestKey], vetoByKey[bestKey], best
}

// stateKeyOf serializa un inventario+vetos a una clave comparable y
// determinística (ordenada), para poder agrupar respuestas idénticas.
// Entrada: inventario, vetos. Salida: string clave.
func stateKeyOf(inv []store.Item, vetos []store.VetoEntry) string {
	invCp := append([]store.Item{}, inv...)
	sortItemsLocal(invCp)
	vetoCp := append([]store.VetoEntry{}, vetos...)
	sortVetosLocal(vetoCp)

	var b strings.Builder
	for _, it := range invCp {
		fmt.Fprintf(&b, "%s=%d;", it.Nombre, it.Cantidad)
	}
	b.WriteString("|")
	for _, v := range vetoCp {
		fmt.Fprintf(&b, "%s=%d;", v.Persona, v.Counter)
	}
	return b.String()
}

// sortItemsLocal ordena un slice de Items por nombre, in-place.
// Entrada: slice de Items. Salida: ninguna (modifica in-place).
func sortItemsLocal(items []store.Item) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j-1].Nombre > items[j].Nombre; j-- {
			items[j-1], items[j] = items[j], items[j-1]
		}
	}
}

// sortVetosLocal ordena un slice de VetoEntry por persona, in-place.
// Entrada: slice de VetoEntry. Salida: ninguna (modifica in-place).
func sortVetosLocal(vetos []store.VetoEntry) {
	for i := 1; i < len(vetos); i++ {
		for j := i; j > 0 && vetos[j-1].Persona > vetos[j].Persona; j-- {
			vetos[j-1], vetos[j] = vetos[j], vetos[j-1]
		}
	}
}

// appendLog escribe una línea en el log de instrucciones del proceso.
// Entrada: instrucción original, resultado obtenido. Salida: ninguna.
func (p *Process) appendLog(instruction, result string) {
	os.MkdirAll("logs", 0755)
	f, err := os.OpenFile(p.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	line := instruction
	if result != "" {
		line += " | " + result
	}
	fmt.Fprintln(f, line)
}

// writeVetoLog escribe el estado final de vetos en su archivo de log.
// Entrada: ninguna. Salida: ninguna.
func (p *Process) writeVetoLog() {
	os.MkdirAll("logs", 0755)
	vetos := p.st.GetVetos()
	f, err := os.Create(p.vetoLogPath)
	if err != nil {
		return
	}
	defer f.Close()
	for _, v := range vetos {
		fmt.Fprintf(f, "VETADO %s %d\n", v.Persona, v.Counter)
	}
}

// infectionFlagPath retorna la ruta del archivo flag de infección de la
// MÁQUINA (no del proceso individual): todos los procesos de la misma
// máquina comparten el mismo flag.
// Entrada: ninguna. Salida: path string.
func (p *Process) infectionFlagPath() string {
	return ".infectado"
}

// infectionFlagExists revisa si existe el flag de infección de la máquina.
// Entrada: ninguna. Salida: bool.
func (p *Process) infectionFlagExists() bool {
	_, err := os.Stat(p.infectionFlagPath())
	return err == nil
}

// RefreshInfectedFromDisk vuelve a leer el flag de infección de la máquina
// y actualiza el estado interno en consecuencia. Se usa como reacción a
// SIGUSR1, manteniendo disco y memoria sincronizados.
// Entrada: ninguna. Salida: ninguna.
func (p *Process) RefreshInfectedFromDisk() {
	p.SetInfected(p.infectionFlagExists())
}

// SetInfected activa o desactiva el modo "infectado" de este proceso.
// Cuando está infectado, este proceso responde con datos falsos a CUALQUIER
// QueryInventory que reciba, sin importar de qué proceso se trate ni quién
// pregunte (incluso si pregunta otro proceso de la misma máquina).
// Entrada: bool. Salida: ninguna.
func (p *Process) SetInfected(v bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.infected = v
	log.Printf("[P%d] Modo infectado (máquina M%d): %v", p.ProcessID, p.MachineID, v)
}

// IsInfected informa si el proceso está actualmente en modo infectado.
// Entrada: ninguna. Salida: bool.
func (p *Process) IsInfected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.infected
}

// Status imprime el inventario, vetos, y la tabla de peers conocidos por
// stdout.
// Entrada: ninguna. Salida: ninguna (escribe a stdout).
func (p *Process) Status() {
	inv := p.st.GetInventory()
	vetos := p.st.GetVetos()
	fmt.Printf("=== Estado M%dP%d ===\n", p.MachineID, p.ProcessID)
	fmt.Println("Inventario propio:")
	for _, item := range inv {
		fmt.Printf("  %s: %d\n", item.Nombre, item.Cantidad)
	}
	fmt.Println("Vetos propios:")
	for _, v := range vetos {
		fmt.Printf("  %s (counter=%d)\n", v.Persona, v.Counter)
	}
}

// fakeItems genera un inventario inventado, usado por procesos infectados
// al responder QueryInventory.
// Entrada: ninguna. Salida: slice de *grpcapi.Item con datos falsos.
func fakeItems() []*grpcapi.Item {
	return []*grpcapi.Item{
		{Nombre: "manzana", Cantidad: int32(99999 + rand.Intn(500))},
		{Nombre: "splicer_residuo", Cantidad: int32(rand.Intn(666))},
	}
}

// fakeVetos genera una lista de vetos inventada, usada por procesos
// infectados al responder QueryInventory.
// Entrada: ninguna. Salida: slice de *grpcapi.VetoEntry con datos falsos.
func fakeVetos() []*grpcapi.VetoEntry {
	return []*grpcapi.VetoEntry{
		{Persona: "splicer_fantasma", Counter: 99},
	}
}

// toProtoItems convierte items del store (struct local) a items proto.
// Entrada: slice de store.Item. Salida: slice de *grpcapi.Item.
func toProtoItems(items []store.Item) []*grpcapi.Item {
	out := make([]*grpcapi.Item, 0, len(items))
	for _, it := range items {
		out = append(out, &grpcapi.Item{Nombre: it.Nombre, Cantidad: int32(it.Cantidad)})
	}
	return out
}

// toProtoVetos convierte vetos del store a vetos proto.
// Entrada: slice de store.VetoEntry. Salida: slice de *grpcapi.VetoEntry.
func toProtoVetos(vetos []store.VetoEntry) []*grpcapi.VetoEntry {
	out := make([]*grpcapi.VetoEntry, 0, len(vetos))
	for _, v := range vetos {
		out = append(out, &grpcapi.VetoEntry{Persona: v.Persona, Counter: int32(v.Counter)})
	}
	return out
}

// fromProtoItems convierte items proto recibidos por gRPC a items del store.
// Entrada: slice de *grpcapi.Item. Salida: slice de store.Item.
func fromProtoItems(items []*grpcapi.Item) []store.Item {
	out := make([]store.Item, 0, len(items))
	for _, it := range items {
		out = append(out, store.Item{Nombre: it.Nombre, Cantidad: int(it.Cantidad)})
	}
	return out
}

// fromProtoVetos convierte vetos proto recibidos por gRPC a vetos del store.
// Entrada: slice de *grpcapi.VetoEntry. Salida: slice de store.VetoEntry.
func fromProtoVetos(vetos []*grpcapi.VetoEntry) []store.VetoEntry {
	out := make([]store.VetoEntry, 0, len(vetos))
	for _, v := range vetos {
		out = append(out, store.VetoEntry{Persona: v.Persona, Counter: int(v.Counter)})
	}
	return out
}

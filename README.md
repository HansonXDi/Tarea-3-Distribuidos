# Tarea 3 – INF-343 Sistemas Distribuidos: ¿Y El Pensador?

## Integrantes

| Nombre | Apellido | Rol |
|--------|----------|-----|
| Erick | Avila | 202273103-6 |
| Hans | Villouta | 202273052-8 |
| Emilio | Valdebenito | 202273040-4 |

---

## Arquitectura de comunicación

### Protocolo elegido: gRPC + Protocol Buffers

Se eligió **gRPC** como protocolo de comunicación entre procesos por las siguientes razones:

- **Tipado fuerte:** Protocol Buffers define los mensajes de forma estricta, eliminando ambigüedades en la serialización y garantizando que todos los procesos interpreten el mismo dato de la misma manera.
- **Eficiencia:** gRPC usa HTTP/2, lo que permite multiplexar múltiples llamadas concurrentes sobre una sola conexión TCP, reduciendo la latencia respecto a REST sobre HTTP/1.1.
- **Generación de código:** `protoc` genera automáticamente tanto el cliente como el servidor en Go, minimizando errores de implementación manual.
- **Bidireccionalidad:** a diferencia de REST donde el servidor es pasivo, gRPC permite que cualquier proceso llame a cualquier otro de forma simétrica, lo cual se ajusta al modelo sin jerarquía de líder que requiere la tarea.

El servicio expuesto por cada proceso (`ExpendedoraService`, en `proto/expendedora.proto`) define tres RPCs:

| RPC | Dirección | Propósito |
|-----|-----------|-----------|
| `Health` | cualquiera → este proceso | Verificar disponibilidad antes de iniciar instrucciones |
| `PushInventory` | este proceso → todos | Notificar cambio de inventario o vetos propios |
| `QueryInventory` | proceso en recuperación → todos | Pedir la copia almacenada de un proceso específico |

### Esquema de puertos

Cada proceso escucha en el puerto `8000 + (machineID - 1) * 100 + processID`.

| Máquina | IP | Procesos (N=7) | Puertos |
|---------|----|-----------------|---------|
| 1 | 10.10.28.35 | P1–P7 | 8001–8007 |
| 2 | 10.10.28.36 | P1–P7 | 8101–8107 |
| 3 | 10.10.28.37 | P1–P7 | 8201–8207 |

---

## Decisiones de diseño y justificación

### 1. Modelo sin líder ni grupos de réplica

Cada proceso `M<m>P<p>` es una entidad **completamente independiente**: tiene su propio inventario, su propia lista de vetos y su propia secuencia de instrucciones. No existe un líder ni una agrupación de réplicas.

**Justificación:** la tarea exige que no exista elección de líder ademas del hecho de que el estado esté replicado pasivamente y que la recuperación se haga por quorum. Un diseño sin líder simplifica la implementación, evita el problema de split-brain durante fallos de red, y cumple exactamente con el enunciado.

### 2. Replicación mediante broadcast pasivo con número de secuencia

Cuando un proceso modifica su inventario o vetos (tras `VETAR`, `COMPRAR` válido, `PERDONAR`, o cualquier decremento de counter), llama a `broadcastInventory()`, que captura el estado actual y un **número de secuencia** (`seq = time.Now().UnixNano()`) **antes** de lanzar las goroutines de envío. Cada peer receptor descarta el mensaje si su `seq` es menor o igual al último ya almacenado.

**Justificación:** el broadcast asíncrono es eficiente (no bloquea la ejecución de instrucciones), pero sin número de secuencia los reintentos tardíos podían pisar un estado más reciente con uno stale, causando inconsistencias en los counters de vetos entre procesos. El `seq` garantiza que cada peer siempre conserva el estado más reciente recibido, independientemente del orden de llegada de los mensajes.

### 3. Decremento de vetos con broadcast obligatorio

El counter de cada veto se decrementa en **cada instrucción `COMPRAR`**, incluso si la compra resulta `NO VALIDO` o `DENEGADO`. Se eligió propagar este decremento siempre (no solo cuando la compra es `VALIDO`) porque:

- El enunciado establece que el counter dura 5 **instrucciones**, no 5 compras exitosas.
- Si no se propagara el decremento en compras fallidas, los counters divergirían entre procesos: un proceso podría tener counter=2 mientras otro tiene counter=4 para la misma persona, lo que violaría la consistencia de estado exigida.

**Implementación:** `DecrementVetos()` retorna `(pardoned []string, anyChanged bool)`; `anyChanged` es `true` si había al menos un veto activo, lo que fuerza el broadcast aunque nadie haya sido perdonado en esa instrucción.

### 4. Recuperación por quorum 2/3

Cuando un proceso se restaura, consulta a **todos los demás procesos del sistema** qué copia tienen de él. Agrupa las respuestas y elige la que se repite más veces. Si esa mayoría alcanza **2/3 del total de procesos consultables**, se adopta ese estado; de lo contrario se reporta error de integridad.

**Justificación:** el enunciado exige explícitamente el umbral de 2/3. Con 3 máquinas y hasta 1 infectada, el quorum garantiza que la máquina sana y la máquina caída (que respondió antes de morir) siempre superan el umbral frente a 1 máquina infectada.

### 5. Modo infectado por máquina (flag en disco)

La infección se gestiona mediante un archivo flag (`.infectado`) en el directorio de trabajo. Todos los procesos de una misma máquina comparten ese flag. Al detectarlo, cualquier `QueryInventory` entrante recibe datos falsos en la respuesta.

**Justificación:** el enunciado pide infectar **todos los procesos activos de la máquina** con un solo comando. Usar un flag en disco es atómico desde el punto de vista del sistema operativo y permite que los procesos lo lean en caliente (vía `SIGUSR1`) sin necesidad de reiniciarse.

### 6. Secciones críticas con `sync.RWMutex`

Tanto `Store` (inventario y vetos propios) como `PeerTable` (copias de otros procesos) usan `sync.RWMutex`:

- Lecturas concurrentes simultáneas (`RLock`) para `GetInventory`, `GetVetos`, `Get`, `Snapshot`.
- Escritura exclusiva (`Lock`) para `Buy`, `Veto`, `Pardon`, `DecrementVetos`, `Update`.

**Justificación:** el enunciado requiere que los procesos ejecuten instrucciones y atiendan `PushInventory` de forma concurrente. Sin mutex, las escrituras concurrentes al inventario o la tabla de peers producirían data races. `RWMutex` permite máxima concurrencia en lectura (que es la operación más frecuente) sin sacrificar correctitud.

---

## Estructura del repositorio

```
.
├── proto/
│   └── expendedora.proto          # Definición del servicio y mensajes gRPC
├── generate_proto.sh              # Genera internal/grpcapi/*.pb.go
├── cmd/
│   └── main.go                    # Punto de entrada: parsea argumentos y arranca el proceso
├── internal/
│   ├── process/
│   │   └── process.go             # Ciclo de vida, broadcast con seq, ejecución, recuperación por quorum
│   ├── store/
│   │   ├── store.go               # Inventario y vetos propios del proceso
│   │   └── peertable.go           # Copias de solo lectura de todos los demás procesos (con seq anti-stale)
│   └── grpcapi/
│       ├── server.go              # Implementación del servicio gRPC
│       ├── client.go              # Cliente gRPC
│       ├── expendedora.pb.go      # GENERADO por protoc (no commitear)
│       └── expendedora_grpc.pb.go # GENERADO por protoc (no commitear)
├── instrucciones/                 # Archivos proceso_<ID>.txt
├── inventario/                    # Plantillas de inventario JSON
├── logs/                          # Logs e inventarios propios (generados en ejecución)
├── script.sh                      # Script principal de control
├── iniciar.sh                     # Script auxiliar para ESTADO
├── go.mod
└── README.md
```

---

## Instrucciones de uso

### Prerequisitos (SOLO SI SE BORRA TODO en cada VM)


```bash
git clone <URL_PRIVADA> tarea3
cd tarea3
chmod +x script.sh iniciar.sh generate_proto.sh
./generate_proto.sh
go mod tidy
```



### 1. Inicializar procesos (ejecutar en cada máquina)

```bash
./script.sh <NUMERO_DE_MAQUINA> <CANTIDAD_DE_PROCESOS>

# Ejemplo: levantar 7 procesos en la máquina 1
./script.sh 1 7
```

Ejecutar el comando correspondiente en cada una de las 3 VMs. Los procesos esperarán hasta 2 segundos para que los demás respondan `Health` antes de comenzar a ejecutar instrucciones.

### 2. Restaurar un proceso caído

```bash
./script.sh <NUMERO_DE_MAQUINA> RESTAURAR <NUMERO_DE_ID_DEL_TXT>

# Ejemplo: restaurar el proceso que lee proceso_4.txt en la máquina 3
./script.sh 3 RESTAURAR 4
```

El proceso recuperado consulta a todos los demás por quorum (2/3) y reconstruye su estado sin re-ejecutar instrucciones.

### 3. Matar un proceso específico

```bash
./script.sh <NUMERO_DE_MAQUINA> MATAR <NUMERO_DE_ID_DEL_TXT>

# Ejemplo: matar el proceso que lee proceso_4.txt en la máquina 3
./script.sh 3 MATAR 4
```

### 4. Infectar / desinfectar esta máquina (toggle)

```bash
./script.sh INFECTAR
```

Ejecutar en la máquina que se desea infectar. Todos sus procesos activos empezarán a responder `QueryInventory` con datos falsos. Ejecutar nuevamente para desinfectar.

### 5. Matar todos los procesos de una máquina

```bash
./script.sh <NUMERO_DE_MAQUINA> KILLALL

# Ejemplo: matar todos los procesos de la máquina 2
./script.sh 2 KILLALL
```

### 6. Ver el estado de un proceso

```bash
./iniciar.sh <NUMERO_DE_MAQUINA> ESTADO <NUMERO_DE_ID_DEL_TXT>

# Ejemplo: ver inventario y vetos del proceso que lee proceso_1.txt en la máquina 3
./iniciar.sh 3 ESTADO 1
```

---

## Formato de los archivos de log

### `logs/inventario_M<m>P<p>.log`

Una línea por instrucción ejecutada. Ejemplo:

```
VETAR jack
COMPRAR jack manzana 10 | DENEGADO
COMPRAR anna dewitt manzana 15 | NO VALIDO
COMPRAR anna dewitt manzana 50 | NO VALIDO
COMPRAR atlas ADAM 5 | NO VALIDO
```

### `logs/vetos_M<m>P<p>.log`

Estado de vetos al finalizar las instrucciones. Ejemplo:

```
VETADO jack 3
VETADO sofia lamb 5
```

### `logs/inventario_propio_M<m>P<p>.json`

Inventario propio persistido en disco (formato JSON). Ejemplo:

```json
[
  {"nombre": "manzana", "cantidad": 85},
  {"nombre": "naranja", "cantidad": 10}
]
```

### `logs/stdout_M<m>P<p>.log`

Salida estándar completa del proceso, incluida la traza de recuperación por quorum.

---

## Mecanismos de tolerancia a fallos

| Problema | Mecanismo implementado |
|----------|------------------------|
| Pérdida de estado al apagar un proceso | Inventario propio persistido en `logs/inventario_propio_M<m>P<p>.json`; PeerTable persistida en `logs/peer_*_copia_*.json` |
| Recuperación de un proceso caído | `QueryInventory` a todos + quorum 2/3; si no se alcanza, error de integridad |
| Inventarios corruptos enviados por máquinas infectadas | Quorum 2/3: la mayoría honesta supera a la máquina infectada |
| Counters de vetos inconsistentes entre procesos | Número de secuencia (`seq`) en cada broadcast; los peers descartan mensajes con `seq` menor al ya almacenado |
| Condiciones de carrera en inventario y vetos | `sync.RWMutex` en `Store` y `PeerTable` |
| Compras fallidas sin propagación de decremento | `DecrementVetos` siempre retorna `anyChanged`; el broadcast ocurre aunque la compra sea `NO VALIDO` |

---

## Uso de IA

Se utilizó asistencia de IA (Claude, Anthropic) en las siguientes secciones:

- Diseño del archivo `.proto` y la estructura del servicio gRPC.
- Estructura de `PeerTable` y la lógica de recuperación por quorum.
- Identificación y corrección del bug de inconsistencia de vetos (broadcasts stale sin número de secuencia).
- Script bash.

Todos los comentarios automáticos generados por IA fueron revisados y reescritos por el grupo. No se incluyeron comentarios automáticos sin revisión.

---

## Consideraciones especiales

- Los archivos `expendedora.pb.go` y `expendedora_grpc.pb.go` son **generados**; no se deben editar manualmente ni commitear. Si se modifica `proto/expendedora.proto`, ejecutar `./generate_proto.sh` y recompilar.
- La constante `totalProcesosPorMaquina` en `cmd/main.go` debe coincidir exactamente en las 3 VMs y con el argumento que se pasa a `./script.sh`.
- Los puertos `8001`–`8X07` (según N procesos por máquina) deben estar abiertos entre las 3 VMs en el firewall.
- El flag de infección (`.infectado`) vive en el directorio de trabajo; ejecutar siempre `script.sh` desde la raíz del repositorio.
- Al restaurar un proceso, se espera hasta 3 segundos para recibir las respuestas de `QueryInventory` de todos los peers antes de evaluar el quorum, tal como especifica el enunciado.

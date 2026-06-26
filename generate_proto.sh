#!/usr/bin/env bash
# generate_proto.sh - Genera el código Go (mensajes + servicio gRPC) a partir
# de proto/expendedora.proto. DEBE ejecutarse al menos una vez antes de
# compilar el proyecto, y cada vez que se modifique el archivo .proto.
#
# Requisitos (instalar antes de ejecutar este script):
#   1. Compilador protoc:
#        sudo apt-get update && sudo apt-get install -y protobuf-compiler
#
#   2. Plugins de Go para protoc:
#        go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#        go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
#      Esto instala los binarios en $(go env GOPATH)/bin (normalmente
#      ~/go/bin). Asegúrate de que esa carpeta esté en tu $PATH:
#        export PATH="$PATH:$(go env GOPATH)/bin"
#
# Uso:
#   ./generate_proto.sh

set -euo pipefail

if ! command -v protoc &> /dev/null; then
    echo "ERROR: protoc no está instalado. Ejecuta:"
    echo "  sudo apt-get update && sudo apt-get install -y protobuf-compiler"
    exit 1
fi

if ! command -v protoc-gen-go &> /dev/null; then
    echo "ERROR: protoc-gen-go no está instalado o no está en el PATH. Ejecuta:"
    echo "  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest"
    echo "  export PATH=\"\$PATH:\$(go env GOPATH)/bin\""
    exit 1
fi

if ! command -v protoc-gen-go-grpc &> /dev/null; then
    echo "ERROR: protoc-gen-go-grpc no está instalado o no está en el PATH. Ejecuta:"
    echo "  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest"
    echo "  export PATH=\"\$PATH:\$(go env GOPATH)/bin\""
    exit 1
fi

echo "[generate_proto] Generando código a partir de proto/expendedora.proto..."

protoc \
    --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    --proto_path=proto \
    proto/expendedora.proto

mkdir -p internal/grpcapi
mv -f expendedora.pb.go internal/grpcapi/ 2>/dev/null || true
mv -f expendedora_grpc.pb.go internal/grpcapi/ 2>/dev/null || true

echo "[generate_proto] Listo. Archivos generados en internal/grpcapi/:"
ls -la internal/grpcapi/*.pb.go

echo "[generate_proto] Ejecuta 'go mod tidy' a continuación para resolver dependencias."

#!/bin/bash

###############################################################################
# Script de Inicialização do Apache Kafka com KRaft
# Autor: Assistant
# Data: 2025-09-23
# Objetivo: Automatizar o processo de inicialização do Kafka usando KRaft
###############################################################################

# Configurações
# Usar variável de ambiente KAFKA_HOME se definida, senão usar padrão
KAFKA_HOME="${KAFKA_HOME:-/home/airflow/kafka}"
CONFIG_FILE="$KAFKA_HOME/config/kraft/server.properties"
LOG_DIR="/tmp/kraft-combined-logs"
LOG_FILE="$KAFKA_HOME/kafka.log"

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para imprimir mensagens coloridas
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Função para verificar se um comando existe
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Função para verificar se o Kafka está rodando
kafka_is_running() {
    pgrep -f "kafka.server.KafkaRaftServer" > /dev/null
}

# Função para verificar se a porta está aberta
port_is_open() {
    local port=$1
    ss -lntp | grep ":$port " > /dev/null 2>&1
}

# Função para verificar se o Kafka está completamente pronto
kafka_is_ready() {
    print_info "🔍 Testando prontidão do Kafka..."
    
    # 1. Verificar se o processo está rodando
    if ! kafka_is_running; then
        print_warning "❌ Teste 1 falhou: Processo não está rodando"
        return 1
    fi
    print_info "✅ Teste 1 passou: Processo está rodando"
    
    # 2. Verificar se a porta 9092 está aberta
    if ! port_is_open 9092; then
        print_warning "❌ Teste 2 falhou: Porta 9092 não está aberta"
        return 1
    fi
    print_info "✅ Teste 2 passou: Porta 9092 está aberta"
    
    # 3. Testar conectividade com kafka-topics (com timeout)
    print_info "🔍 Teste 3: Testando kafka-topics..."
    timeout 10s bin/kafka-topics.sh --bootstrap-sernão agregações globaisver localhost:9092 --list > /dev/null 2>&1
    local topics_result=$?
    
    if [ $topics_result -eq 0 ]; then
        print_info "✅ Teste 3 passou: kafka-topics funcionando"
        return 0
    else
        print_warning "❌ Teste 3 falhou: kafka-topics retornou $topics_result"
        # Mostrar erro para debug
        print_info "🔍 Debug: Testando kafka-topics com output..."
        timeout 5s bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>&1 | head -3
        return 1
    fi
}

# Função para aguardar Kafka ficar pronto com timeout
wait_for_kafka_ready() {
    local timeout=${1:-45}  # timeout reduzido para 45 segundos
    local interval=${2:-3}  # intervalo aumentado para 3 segundos
    local elapsed=0
    
    print_info "Aguardando Kafka ficar pronto (timeout: ${timeout}s, intervalo: ${interval}s)..."
    
    while [ $elapsed -lt $timeout ]; do
        if kafka_is_ready; then
            print_success "Kafka está pronto! (tempo decorrido: ${elapsed}s)"
            return 0
        fi
        
        # Mostrar progresso a cada 15 segundos
        if [ $((elapsed % 15)) -eq 0 ] && [ $elapsed -gt 0 ]; then
            print_info "Ainda aguardando... (${elapsed}s/${timeout}s)"
        fi
        
        sleep $interval
        elapsed=$((elapsed + interval))
    done
    
    print_error "Timeout: Kafka não ficou completamente pronto em ${timeout} segundos"
    
    # Verificação final para ver se o Kafka está pelo menos rodando
    if kafka_is_running; then
        print_warning "⚠️ O processo Kafka ESTÁ rodando, mas pode ainda estar inicializando"
        print_info "💡 O Kafka pode estar funcional mesmo com este timeout"
        print_info "💡 Tente executar: $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
        return 2  # Retorna código diferente para timeout com processo rodando
    else
        print_error "❌ O processo Kafka não está rodando"
        return 1
    fi
}

# Função para parar o Kafka se estiver rodando
stop_kafka() {
    if kafka_is_running; then
        print_warning "Kafka está rodando. Parando o serviço..."
        pkill -f "kafka.server.KafkaRaftServer"
        sleep 5
        
        if kafka_is_running; then
            print_warning "Forçando parada do Kafka..."
            pkill -9 -f "kafka.server.KafkaRaftServer"
            sleep 2
        fi
        
        if ! kafka_is_running; then
            print_success "Kafka parado com sucesso"
        else
            print_error "Não foi possível parar o Kafka"
            exit 1
        fi
    fi
}

# Função principal
main() {
    # Verificar se deve forçar formatação
    FORCE_FORMAT=false
    if [ "$1" = "--format" ]; then
        FORCE_FORMAT=true
        print_warning "Formatação forçada solicitada via --format"
    fi
    
    print_info "=== INICIANDO CONFIGURAÇÃO DO KAFKA KRAFT ==="
    
    # Verificar se estamos no diretório correto
    if [ ! -d "$KAFKA_HOME" ]; then
        print_error "Diretório do Kafka não encontrado: $KAFKA_HOME"
        exit 1
    fi
    
    cd "$KAFKA_HOME" || exit 1
    print_info "Mudando para diretório: $KAFKA_HOME"
    
    # Verificar se o arquivo de configuração existe
    if [ ! -f "$CONFIG_FILE" ]; then
        print_error "Arquivo de configuração não encontrado: $CONFIG_FILE"
        exit 1
    fi
    
    # Verificar se o Java está instalado
    if ! command_exists java; then
        print_error "Java não está instalado. Por favor, instale o Java primeiro."
        exit 1
    fi
    
    print_success "Verificações iniciais concluídas"
    
    # Parar Kafka se estiver rodando
    stop_kafka
    
    # Verificar se precisa formatar o storage
    NEEDS_FORMAT=false
    
    if [ "$FORCE_FORMAT" = true ]; then
        print_info "Formatação forçada via --format"
        NEEDS_FORMAT=true
    elif [ ! -d "$LOG_DIR" ]; then
        print_info "Diretório de metadata não existe, formatação necessária"
        NEEDS_FORMAT=true
    else
        print_info "Metadata já existe, pulando formatação"
    fi
    
    if [ "$NEEDS_FORMAT" = true ]; then
        # Limpar diretório de logs antigos
        print_info "Limpando diretório de logs antigos..."
        if [ -d "$LOG_DIR" ]; then
            rm -rf "$LOG_DIR"
            print_success "Diretório de logs limpo: $LOG_DIR"
        fi
        
        # Gerar UUID único para o cluster
        print_info "Gerando UUID único para o cluster..."
        CLUSTER_UUID=$(bin/kafka-storage.sh random-uuid)
        if [ $? -eq 0 ]; then
            print_success "UUID gerado: $CLUSTER_UUID"
        else
            print_error "Falha ao gerar UUID"
            exit 1
        fi
        
        # Formatar o diretório de metadata
        print_info "Formatando diretório de metadata..."
        bin/kafka-storage.sh format -t "$CLUSTER_UUID" -c "$CONFIG_FILE"
        if [ $? -eq 0 ]; then
            print_success "Metadata formatado com sucesso"
        else
            print_error "Falha ao formatar metadata"
            exit 1
        fi
    fi
    
    # Iniciar o servidor Kafka
    print_info "Iniciando servidor Kafka KRaft..."
    nohup bin/kafka-server-start.sh "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
    KAFKA_PID=$!
    
    # Aguardar Kafka ficar completamente pronto
    wait_for_kafka_ready 45 3
    wait_result=$?
    
    if [ "$wait_result" -eq 0 ]; then
        print_success "Kafka iniciado e totalmente pronto para uso!"
        print_info "PID do processo: $KAFKA_PID"
        print_info "Logs disponíveis em: $LOG_FILE"
        print_info "Configuração utilizada: $CONFIG_FILE"
        print_info "Servidor disponível em: localhost:9092"
        
    elif [ "$wait_result" -eq 2 ]; then
        print_warning "Kafka iniciado com timeout na validação, mas processo está rodando!"
        print_info "PID do processo: $KAFKA_PID"
        print_info "Logs disponíveis em: $LOG_FILE"
        print_info "Servidor provavelmente disponível em: localhost:9092"
        
        print_info "🔍 Fazendo teste final manual..."
        if timeout 10s bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
            print_success "✅ Teste manual passou - Kafka está funcionando!"
            
            # Criar tópico 'sales' automaticamente
            print_info "📝 Criando tópico 'sales' automaticamente..."
            if timeout 15s bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic sales --partitions 6 --replication-factor 1 >/dev/null 2>&1; then
                print_success "✅ Tópico 'sales' criado com sucesso!"
            else
                print_warning "⚠️ Tópico 'sales' pode já existir ou houve problema na criação"
            fi
        else
            print_warning "⚠️ Teste manual falhou - Kafka pode ainda estar inicializando"
        fi
        
    else
        print_error "Falha ao iniciar o Kafka - processo não está rodando"
        print_error "Verifique os logs em: $LOG_FILE"
        
        # Mostrar últimas linhas do log para diagnóstico
        if [ -f "$LOG_FILE" ]; then
            print_info "Últimas linhas do log:"
            tail -n 20 "$LOG_FILE"
        fi
        
        exit 1
    fi
    
    print_info "=== CONFIGURAÇÃO CONCLUÍDA ==="
    echo
    print_info "Comandos úteis:"
    echo "  - Verificar status: ps aux | grep kafka"
    echo "  - Ver logs: tail -f $LOG_FILE"
    echo "  - Listar tópicos: $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"
    echo "  - Parar Kafka: pkill -f 'kafka.server.KafkaRaftServer'"
}

# Executar função principal
main "$@"
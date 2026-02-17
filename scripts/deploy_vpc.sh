#!/bin/bash
# ============================================================
# PolyEdge VPC 部署脚本
# 用于东京和爱尔兰服务器的部署
#
# 使用:
#   # 东京服务器 (数据源)
#   ./deploy_vpc.sh tokyo
#
#   # 爱尔兰服务器 (执行引擎)
#   ./deploy_vpc.sh ireland
# ============================================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置
TOKYO_IP="172.31.44.26"
IRELAND_IP="10.0.3.123"
TARGET_PORT=6666
FEEDER_PORT=9999

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查依赖
check_dependencies() {
    log_info "检查依赖..."

    if ! command -v cargo &> /dev/null; then
        log_error "Rust 未安装"
        exit 1
    fi

    if ! command -v python3 &> /dev/null; then
        log_error "Python3 未安装"
        exit 1
    fi

    log_success "依赖检查完成"
}

# 部署东京服务器
deploy_tokyo() {
    log_info "部署东京服务器..."

    # 编译 release 版本
    log_info "编译 feeder_tokyo..."
    cargo build -p feeder_tokyo --release

    # 创建启动脚本
    cat > /tmp/feeder_tokyo.sh << 'EOF'
#!/bin/bash
export SYMBOL_TARGETS="btcusdt=10.0.3.123:6666,ethusdt=10.0.3.123:6667,solusdt=10.0.3.123:6668,xrpusdt=10.0.3.123:6669"
export BIND_BASE_PORT="9999"
export POLYEDGE_UDP_REDUNDANCY="2"
export POLYEDGE_UDP_SNDBUF_BYTES="16777216"
export POLYEDGE_SENDER_PIN_CORES="btcusdt:0,ethusdt:0,solusdt:1,xrpusdt:1"

cd /opt/PolyEdge
./target/release/sender >> /var/log/feeder_tokyo.log 2>&1
EOF

    chmod +x /tmp/feeder_tokyo.sh

    log_success "东京服务器部署完成"
    log_info "启动命令: export SYMBOL_TARGETS='btcusdt=10.0.3.123:6666,ethusdt=10.0.3.123:6667,solusdt=10.0.3.123:6668,xrpusdt=10.0.3.123:6669' && export POLYEDGE_UDP_REDUNDANCY=2 && cargo run -p feeder_tokyo --bin sender --release"
}

# 部署爱尔兰服务器
deploy_ireland() {
    log_info "部署爱尔兰服务器..."

    # 确保端口开放
    log_info "检查端口 $TARGET_PORT ..."

    # 编译 release 版本
    log_info "编译 PolyEdge..."
    cargo build -p app_runner --release

    log_success "爱尔兰服务器部署完成"
    log_info "启动命令: cargo run -p app_runner --release"

    # 设置环境变量
    cat > /tmp/polyedge.env << EOF
# PolyEdge 环境变量
export POLYEDGE_BINANCE_RELAY=true
export POLYEDGE_BINANCE_RELAY_HOST=127.0.0.1
export POLYEDGE_BINANCE_RELAY_PORT=6666
export POLYEDGE_UDP_SYMBOL_PORTS="BTCUSDT:6666,ETHUSDT:6667,SOLUSDT:6668,XRPUSDT:6669"
export POLYEDGE_UDP_RCVBUF_BYTES=26214400
export POLYEDGE_UDP_BUSY_POLL_US=50
export POLYEDGE_UDP_PIN_CORES="6666:1,6667:2,6668:2,6669:3"
EOF

    log_info "环境配置已保存到 /tmp/polyedge.env"
}

# 测试网络连通性
test_connectivity() {
    local target=$1
    local port=$2

    log_info "测试连通性: $target:$port"

    if command -v nc &> /dev/null; then
        if nc -zv -w 5 $target $port 2>&1 | grep -q "succeeded"; then
            log_success "连接成功"
            return 0
        else
            log_error "连接失败"
            return 1
        fi
    elif command -v timeout &> /dev/null; then
        if timeout 5 bash -c "echo > /dev/tcp/$target/$port" 2>/dev/null; then
            log_success "连接成功"
            return 0
        else
            log_error "连接失败"
            return 1
        fi
    else
        log_warn "无法测试连通性 (nc/timeout 未安装)"
        return 0
    fi
}

# 测试延迟
test_latency() {
    local target=$1

    log_info "测试延迟到 $target..."

    if command -v ping &> /dev/null; then
        ping -c 5 $target | tail -1
    fi

    if command -v nc &> /dev/null; then
        log_info "使用 nc 测试 TCP 延迟..."
        time nc -zv $target $TARGET_PORT 2>&1 | grep -E "time|connected"
    fi
}

# 启动 systemd 服务 (可选)
setup_systemd() {
    local service_name=$1
    local user=$(whoami)

    log_info "创建 systemd 服务: $service_name"

    cat > /tmp/${service_name}.service << EOF
[Unit]
Description=PolyEdge $service_name
After=network.target

[Service]
Type=simple
User=$user
WorkingDirectory=/opt/PolyEdge
Environment=TARGET=10.0.3.123:6666
Environment=SYMBOL_TARGETS=btcusdt=10.0.3.123:6666,ethusdt=10.0.3.123:6667,solusdt=10.0.3.123:6668,xrpusdt=10.0.3.123:6669
Environment=POLYEDGE_UDP_REDUNDANCY=2
Environment=POLYEDGE_UDP_SNDBUF_BYTES=16777216
Environment=POLYEDGE_SENDER_PIN_CORES=btcusdt:0,ethusdt:0,solusdt:1,xrpusdt:1
ExecStart=/opt/PolyEdge/target/release/sender
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

    log_success "systemd 服务文件已创建: /tmp/${service_name}.service"
    log_info "安装: sudo cp /tmp/${service_name}.service /etc/systemd/system/"
    log_info "启动: sudo systemctl start $service_name"
}

# 主菜单
show_menu() {
    echo ""
    echo "=========================================="
    echo "  PolyEdge VPC 部署工具"
    echo "=========================================="
    echo ""
    echo "选择服务器:"
    echo "  1) 东京服务器 (数据源)"
    echo "  2) 爱尔兰服务器 (执行引擎)"
    echo "  3) 测试连通性 (东京 -> 爱尔兰)"
    echo "  4) 测试延迟"
    echo "  5) 创建 systemd 服务"
    echo "  0) 退出"
    echo ""
    echo -n "选择: "
}

# 主函数
main() {
    local mode=${1:-menu}

    check_dependencies

    case $mode in
        tokyo)
            deploy_tokyo
            ;;
        ireland)
            deploy_ireland
            ;;
        test)
            test_connectivity $TOKYO_IP $TARGET_PORT
            ;;
        latency)
            test_latency $TOKYO_IP
            ;;
        systemd)
            setup_systemd "polyedge-feeder"
            ;;
        menu)
            show_menu
            ;;
        *)
            echo "用法: $0 {tokyo|ireland|test|latency|systemd|menu}"
            exit 1
            ;;
    esac
}

main "$@"

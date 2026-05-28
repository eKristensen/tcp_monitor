BINARY  := target/release/tcp-monitor
DESTBIN := /usr/local/bin/tcp-monitor
DESTSVC := /etc/systemd/system/tcp-monitor.service
CFGDIR  := /etc/tcp-monitor
DESTCFG := $(CFGDIR)/config.toml

.PHONY: all build check install enable firewall uninstall help

all: build

## build     — compile the release binary
build: check
	cargo build --release

## check     — verify required tools are present
check:
	@echo "Checking dependencies..."
	@command -v cargo >/dev/null 2>&1 || { \
		echo "ERROR: cargo not found."; \
		echo "Install Rust via rustup:"; \
		echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"; \
		exit 1; \
	}
	@echo "  cargo : $$(cargo --version)"
	@echo "  rustc : $$(rustc --version)"
	@echo "OK"

## install   — install binary, service file, and example config  [needs root]
install: $(BINARY)
	@[ "$$(id -u)" = 0 ] || { echo "ERROR: run as root: sudo make install"; exit 1; }
	install -Dm755 $(BINARY)           $(DESTBIN)
	install -Dm644 tcp-monitor.service $(DESTSVC)
	install -d                         $(CFGDIR)
	@if [ ! -f $(DESTCFG) ]; then \
		install -Dm644 config.example.toml $(DESTCFG); \
		echo ""; \
		echo "  Config written to $(DESTCFG)"; \
		echo "  Edit it before starting the service."; \
	else \
		echo "  $(DESTCFG) already exists — not overwritten."; \
	fi
	systemctl daemon-reload
	@echo ""
	@echo "Installed. Next steps:"
	@echo "  1. Edit $(DESTCFG)"
	@echo "  2. sudo make firewall   # open required ports"
	@echo "  3. sudo make enable     # start and enable the service"

## enable    — start and enable the service at boot  [needs root]
enable:
	@[ "$$(id -u)" = 0 ] || { echo "ERROR: run as root: sudo make enable"; exit 1; }
	systemctl enable --now tcp-monitor

## firewall  — open heartbeat (9700) and probe (9701) ports in firewalld  [needs root]
#              Metrics port 9702 is scraped locally — no firewall rule needed.
firewall:
	@[ "$$(id -u)" = 0 ] || { echo "ERROR: run as root: sudo make firewall"; exit 1; }
	@command -v firewall-cmd >/dev/null 2>&1 || { echo "ERROR: firewall-cmd not found — is firewalld installed?"; exit 1; }
	firewall-cmd --permanent --add-port=9700/tcp
	firewall-cmd --permanent --add-port=9701/tcp
	firewall-cmd --reload
	@echo "Ports 9700 (heartbeat) and 9701 (probe) are now open."

## uninstall — stop the service and remove the binary and service file  [needs root]
#              The config directory $(CFGDIR) is left in place.
uninstall:
	@[ "$$(id -u)" = 0 ] || { echo "ERROR: run as root: sudo make uninstall"; exit 1; }
	systemctl disable --now tcp-monitor 2>/dev/null || true
	rm -f $(DESTBIN) $(DESTSVC)
	systemctl daemon-reload
	@echo "Removed. Config at $(CFGDIR) was left in place."

## help      — show available targets
help:
	@grep -E '^## ' Makefile | sed 's/^## /  make /'

BIN_DIR ?= bin
VERSION ?= 0.509.0-beta

.PHONY: all
all: build

.PHONY: build
build:
	@python -c "import os, shutil, glob; os.makedirs('$(BIN_DIR)', exist_ok=True)"
	cargo build --release
	@python -c "import os, shutil, glob; [shutil.copy(f, '$(BIN_DIR)/') for f in glob.glob('target/release/*kuiper*') if not f.endswith('.d') and not f.endswith('.pdb') and os.path.isfile(f)]"
	@echo Build successful: binaries copied to $(BIN_DIR)/

.PHONY: check
check:
	cargo check --workspace

.PHONY: test
test:
	cargo test --workspace

.PHONY: run
run:
	cargo run --bin rekuiperd -- --etc etc

.PHONY: clean
clean:
	cargo clean
	rm -rf $(BIN_DIR)

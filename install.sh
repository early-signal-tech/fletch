#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Error handler
cleanup() {
  if [ -n "$TEMP_FILE" ] && [ -f "$TEMP_FILE" ]; then
    rm -f "$TEMP_FILE"
  fi
}
trap cleanup EXIT

error() {
  echo -e "${RED}✗ Error: $1${NC}" >&2
  exit 1
}

info() {
  echo -e "${GREEN}→${NC} $1"
}

warn() {
  echo -e "${YELLOW}⚠${NC}  $1"
}

# Parse arguments
VERSION="${1:-latest}"
INSTALL_PATH="${2:-/usr/local/bin/fletch}"
REPO="early-signal-tech/fletch"

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

# Map architectures
case $ARCH in
  arm64) ARCH="arm64" ;;
  x86_64) ARCH="amd64" ;;
  *) error "Unsupported architecture: $ARCH" ;;
esac

# Map OS
case $OS in
  darwin) OS="darwin" ;;
  linux) OS="linux" ;;
  *) error "Unsupported OS: $OS" ;;
esac

BINARY_NAME="fletch-${OS}-${ARCH}"
TEMP_FILE="/tmp/$BINARY_NAME"

# Check if fletch is already installed
if command -v fletch &> /dev/null; then
  CURRENT_VERSION=$(fletch version 2>/dev/null | grep -oE 'v[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "unknown")
  warn "fletch is already installed (version: $CURRENT_VERSION) at $(command -v fletch)"
  echo "  Continuing with installation to $INSTALL_PATH..."
fi

# Determine actual download tag
if [ "$VERSION" = "latest" ]; then
  info "Fetching latest release..."
  DOWNLOAD_TAG=$(curl -s https://api.github.com/repos/$REPO/releases/latest | grep '"tag_name"' | head -1 | cut -d'"' -f4)
  if [ -z "$DOWNLOAD_TAG" ]; then
    error "Could not fetch latest release from GitHub"
  fi
  info "Latest version: $DOWNLOAD_TAG"
else
  DOWNLOAD_TAG="$VERSION"
fi

# Download binary
DOWNLOAD_URL="https://github.com/$REPO/releases/download/$DOWNLOAD_TAG/$BINARY_NAME"
info "Downloading $BINARY_NAME from $DOWNLOAD_TAG..."

if ! curl -fL "$DOWNLOAD_URL" -o "$TEMP_FILE" 2>/dev/null; then
  error "Failed to download $BINARY_NAME. Check your internet connection or verify the release exists."
fi

# Verify download
if [ ! -f "$TEMP_FILE" ] || [ ! -s "$TEMP_FILE" ]; then
  error "Download appears to be corrupted or empty"
fi

# Make executable
chmod +x "$TEMP_FILE"

# Check if we can write to install path
INSTALL_DIR=$(dirname "$INSTALL_PATH")
if [ ! -w "$INSTALL_DIR" ]; then
  warn "Installation requires elevated permissions"
  if ! sudo -n true 2>/dev/null; then
    info "Enter your password to complete installation:"
  fi
  sudo mv "$TEMP_FILE" "$INSTALL_PATH"
else
  mv "$TEMP_FILE" "$INSTALL_PATH"
fi

# Verify installation
if ! "$INSTALL_PATH" --help &>/dev/null; then
  error "Installation verification failed. Binary may not be compatible with this system."
fi

info "fletch installed successfully to $INSTALL_PATH"
echo ""
echo "Version information:"
"$INSTALL_PATH" version 2>/dev/null || "$INSTALL_PATH" --help | head -3
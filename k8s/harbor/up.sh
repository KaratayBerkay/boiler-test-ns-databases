#!/usr/bin/env bash
# Harbor for the ns-databases lab — the same registry the sibling rd-databases lab uses, one project per lab.
#
#   k8s/harbor/up.sh            if a Harbor already answers at HARBOR_HOST:HARBOR_HTTPS_PORT (the shared instance):
#                               copy its CA into k8s/harbor/certs/ and create the `nslab` project + robot accounts.
#                               Otherwise install one here: download the official online installer, generate a CA +
#                               server cert, render harbor.yml, `prepare`, `docker compose up -d`, wait for health,
#                               then create the project + robots (harbor/setup.py) and record the secrets in k8s/.env
#   k8s/harbor/up.sh --down     stop a Harbor installed by this script (keeps data/ and certs/); `--purge` also deletes data/
#
# Design notes (same as rd-databases/k8s/harbor/up.sh):
#   - Harbor's hostname is the host's LAN IP (HARBOR_HOST): the docker client on the host and the k3s nodes inside k3d
#     must both reach the *same* address, because Harbor puts it into the token realm of every 401 challenge.
#   - No root anywhere: pushes from the host use 127.0.0.1:<port> (loopback is an insecure registry for Docker), k3s
#     nodes get the CA mounted by k3d (k8s/k3d.yaml) and pull with a read-only robot account (registries.yaml).
#   - One Harbor per host is enough: projects isolate the labs (rdlab, nslab), robots are project-scoped.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
K8S="$(dirname "$HERE")"
VERSION="${HARBOR_VERSION:-v2.15.2}"
DIST="$HERE/dist/harbor"
PROJECT_NAME="harbor"                       # docker compose project name of a Harbor installed by this script

[[ -f "$K8S/.env" ]] || { echo "!! $K8S/.env missing: cp k8s/.env.example k8s/.env && chmod 600 k8s/.env" >&2; exit 1; }
set -a; . "$K8S/.env"; set +a
: "${HARBOR_HOST:?}" "${HARBOR_HTTP_PORT:=8880}" "${HARBOR_HTTPS_PORT:=8443}" "${HARBOR_ADMIN_PASSWORD:?}" "${HARBOR_DB_PASSWORD:?}"
CERTS="$HERE/certs"; mkdir -p "$CERTS"; chmod 700 "$CERTS"

compose() { docker compose -p "$PROJECT_NAME" -f "$DIST/docker-compose.yml" "$@"; }
healthy() { curl -fsk "https://127.0.0.1:${HARBOR_HTTPS_PORT}/api/v2.0/health" 2>/dev/null | grep -q '"status":"healthy"'; }

case "${1:-}" in
  --down)  compose down; exit 0 ;;
  --purge) compose down -v; rm -rf "$HERE/data" "$HERE/logs"; echo "harbor stopped, data removed"; exit 0 ;;
  "") ;;
  *) echo "usage: $0 [--down|--purge]" >&2; exit 2 ;;
esac

if healthy; then
  echo "==> harbor already answers at ${HARBOR_HOST}:${HARBOR_HTTPS_PORT} (shared instance) — reusing it"
  if [[ ! -f "$CERTS/ca.crt" ]]; then
    src="${HARBOR_CA_SRC:-}"
    [[ -n "$src" && "$src" != /* ]] && src="$K8S/$src"
    if [[ -n "$src" && -f "$src" ]]; then
      cp "$src" "$CERTS/ca.crt"; echo "    CA copied from $src"
    else
      # no CA file at hand: take the certificate chain the server presents (self-signed CA is the last one)
      openssl s_client -showcerts -connect "127.0.0.1:${HARBOR_HTTPS_PORT}" </dev/null 2>/dev/null \
        | awk '/BEGIN CERT/{c++} c>=1{print}' | awk 'BEGIN{RS="-----END CERTIFICATE-----\n"} NF{last=$0 RS} END{printf "%s", last}' > "$CERTS/ca.crt"
      echo "    CA extracted from the server's certificate chain"
    fi
  fi
else
  # ---------------------------------------------------------------------------------- install (no Harbor on this host yet)
  if [[ ! -x "$DIST/prepare" ]]; then
    echo "==> downloading harbor online installer $VERSION"
    mkdir -p "$HERE/dist"
    gh release download "$VERSION" --repo goharbor/harbor --pattern "harbor-online-installer-${VERSION}.tgz" --dir "$HERE/dist" --clobber
    tar xzf "$HERE/dist/harbor-online-installer-${VERSION}.tgz" -C "$HERE/dist"
  fi
  if [[ ! -f "$CERTS/harbor.crt" ]]; then
    echo "==> generating CA and server certificate in $CERTS"
    openssl req -x509 -new -nodes -sha256 -days 3650 -newkey rsa:4096 -subj "/CN=nslab harbor CA" \
      -keyout "$CERTS/ca.key" -out "$CERTS/ca.crt" >/dev/null 2>&1
    cat > "$CERTS/san.cnf" <<CNF
[req]
distinguished_name = dn
req_extensions = ext
prompt = no
[dn]
CN = $HARBOR_HOST
[ext]
subjectAltName = @alt
basicConstraints = CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
[alt]
IP.1 = $HARBOR_HOST
IP.2 = 127.0.0.1
DNS.1 = harbor.nslab.local
DNS.2 = localhost
DNS.3 = host.k3d.internal
CNF
    openssl req -new -nodes -newkey rsa:4096 -keyout "$CERTS/harbor.key" -out "$CERTS/harbor.csr" -config "$CERTS/san.cnf" >/dev/null 2>&1
    openssl x509 -req -sha256 -days 3650 -in "$CERTS/harbor.csr" -CA "$CERTS/ca.crt" -CAkey "$CERTS/ca.key" -CAcreateserial \
      -out "$CERTS/harbor.crt" -extensions ext -extfile "$CERTS/san.cnf" >/dev/null 2>&1
    chmod 600 "$CERTS"/*.key
  fi
  mkdir -p "$HERE/data" "$HERE/logs"
  HARBOR_DIR="$HERE" python3 - "$DIST/harbor.yml.tmpl" "$DIST/harbor.yml" <<'PY'
import os, sys, re
src, dst = sys.argv[1], sys.argv[2]
t = open(src).read()
here = os.environ["HARBOR_DIR"]; certs = os.path.join(here, "certs")
def sub(pattern, repl):
    global t
    t, n = re.subn(pattern, repl, t, count=1, flags=re.M); assert n == 1, pattern
sub(r"^hostname: .*$", f"hostname: {os.environ['HARBOR_HOST']}")
sub(r"^http:\n((?:\s*#.*\n)*)  port: \d+", lambda m: f"http:\n{m.group(1)}  port: {os.environ['HARBOR_HTTP_PORT']}")
sub(r"^https:\n((?:\s*#.*\n)*)  port: \d+", lambda m: f"https:\n{m.group(1)}  port: {os.environ['HARBOR_HTTPS_PORT']}")
sub(r"^  certificate: .*$", f"  certificate: {certs}/harbor.crt")
sub(r"^  private_key: .*$", f"  private_key: {certs}/harbor.key")
sub(r"^harbor_admin_password: .*$", f"harbor_admin_password: {os.environ['HARBOR_ADMIN_PASSWORD']}")
sub(r"^database:\n((?:\s*#.*\n)*)  password: .*$", lambda m: f"database:\n{m.group(1)}  password: {os.environ['HARBOR_DB_PASSWORD']}")
sub(r"^data_volume: .*$", f"data_volume: {os.path.join(here, 'data')}")
sub(r"^    location: /var/log/harbor$", f"    location: {os.path.join(here, 'logs')}")
open(dst, "w").write(t)
print(f"==> rendered {dst} (hostname {os.environ['HARBOR_HOST']}, https {os.environ['HARBOR_HTTPS_PORT']})")
PY
  echo "==> prepare (generates $DIST/common/config and docker-compose.yml)"
  ( cd "$DIST" && HARBOR_BUNDLE_DIR="$DIST" ./prepare >/dev/null )
  docker run --rm --entrypoint sh -v "$DIST:/c" goharbor/prepare:${VERSION} -c \
    "chown $(id -u):$(id -g) /c/docker-compose.yml /c/common/config/*/env" >/dev/null
  sed -i -E 's/^(\s*container_name:\s*)(nginx|redis|registry|registryctl)\s*$/\1harbor-\2/' "$DIST/docker-compose.yml"
  echo "==> docker compose up"
  compose up -d >/dev/null
  echo -n "==> waiting for harbor"
  for i in $(seq 1 90); do
    if healthy; then echo " healthy"; break; fi
    echo -n "."; sleep 2
    [[ $i -eq 90 ]] && { echo; echo "!! harbor not healthy; docker compose -p harbor -f $DIST/docker-compose.yml logs" >&2; exit 1; }
  done
fi

# project + robot accounts for THIS lab ------------------------------------------------------------------------------
python3 "$HERE/setup.py"
echo
echo "  UI        https://${HARBOR_HOST}:${HARBOR_HTTPS_PORT}   (admin / see k8s/.env; self-signed: trust k8s/harbor/certs/ca.crt)"
echo "  push      k8s/push-images.sh          (docker login 127.0.0.1:${HARBOR_HTTPS_PORT} as the pusher robot, isolated docker config)"
echo "  cluster   k8s/up.sh                   (k3d cluster that pulls from ${HARBOR_HOST}:${HARBOR_HTTPS_PORT} with the k3s robot)"

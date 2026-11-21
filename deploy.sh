#!/bin/bash
# deploy.sh — mongo-stream-cdc
# Deploys the full pipeline to Kubernetes in the correct order.
#
# Usage:
#   ./deploy.sh                         # full deploy
#   ./deploy.sh --reinit                # fresh snapshot + restart stream/sink
#   ./deploy.sh --tables Org Campaign   # snapshot specific tables only
#   ./deploy.sh --skip-snap             # skip snapshot, just deploy stream + sink
#   ./deploy.sh --image your-reg/msc:v2 # override image tag

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────
NAMESPACE="mongo-stream-cdc"
IMAGE="your-registry/mongo-stream-cdc:latest"
SKIP_SNAP=false
REINIT=false
TABLES=()
SNAP_TIMEOUT="7200s"

# ── Colors ────────────────────────────────────────────────────────
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
BLUE="\033[0;34m"
NC="\033[0m"

log()  { echo -e "${BLUE}[msc]${NC} $*"; }
ok()   { echo -e "${GREEN}[msc] ✓${NC} $*"; }
warn() { echo -e "${YELLOW}[msc] ⚠${NC} $*"; }
fail() { echo -e "${RED}[msc] ✗${NC} $*"; exit 1; }

# ── Arg Parsing ───────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --skip-snap) SKIP_SNAP=true; shift ;;
    --reinit)    REINIT=true;    shift ;;
    --image)     IMAGE="$2";     shift 2 ;;
    --tables)
      shift
      while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
        TABLES+=("$1"); shift
      done
      ;;
    *) fail "Unknown argument: $1" ;;
  esac
done

# ── Pre-flight ────────────────────────────────────────────────────
log "Checking kubectl context..."
CONTEXT=$(kubectl config current-context)
log "Context: $CONTEXT"
read -p "Continue? [y/N] " -n 1 -r
echo
[[ $REPLY =~ ^[Yy]$ ]] || fail "Aborted."

# ── Update image in manifests ─────────────────────────────────────
update_image() {
  local file=$1
  sed -i "s|your-registry/mongo-stream-cdc:latest|${IMAGE}|g" "$file"
}

log "Setting image: $IMAGE"
update_image k8s/04-job-snap.yaml
update_image k8s/05-deployment-stream.yaml
update_image k8s/06-deployment-sink.yaml

# ── Step 1: Base resources ────────────────────────────────────────
log "Applying namespace..."
kubectl apply -f k8s/00-namespace.yaml

log "Applying secrets..."
kubectl apply -f k8s/01-secrets.yaml

log "Applying configmap..."
kubectl apply -f k8s/02-configmap.yaml

log "Applying PVC..."
kubectl apply -f k8s/03-pvc.yaml

# Load table config into k8s
log "Loading table config into Kubernetes..."
kubectl create configmap msc-table-config \
  --from-file=combined_table_config_3.json=./combined_table_config_3.json \
  --namespace "$NAMESPACE" \
  --dry-run=client -o yaml | kubectl apply -f -

ok "Base resources applied"

# ── Step 2: Handle reinit ─────────────────────────────────────────
if [[ "$REINIT" == "true" ]]; then
  warn "Reinit mode — stopping stream + sink before snapshot..."
  kubectl delete deployment msc-stream msc-sink \
    -n "$NAMESPACE" --ignore-not-found=true
  kubectl delete job msc-snap \
    -n "$NAMESPACE" --ignore-not-found=true
  log "Waiting for pods to terminate..."
  kubectl wait --for=delete pod \
    -l "app in (msc-stream,msc-sink)" \
    -n "$NAMESPACE" --timeout=60s 2>/dev/null || true
fi

# ── Step 3: Snapshot ──────────────────────────────────────────────
if [[ "$SKIP_SNAP" == "false" ]]; then

  # Check if already initialized (resume_point.json exists on PVC)
  ALREADY_INIT=$(kubectl exec -n "$NAMESPACE" \
    $(kubectl get pod -n "$NAMESPACE" -l app=msc-stream -o name 2>/dev/null | head -1) \
    -- test -f /resume/resume_point.json 2>/dev/null && echo "yes" || echo "no")

  if [[ "$ALREADY_INIT" == "yes" && "$REINIT" == "false" ]]; then
    warn "Resume point already exists. Skipping snapshot."
    warn "Use --reinit to force a fresh snapshot."
  else
    log "Starting snapshot job..."

    # Patch tables arg if specific tables requested
    if [[ ${#TABLES[@]} -gt 0 ]]; then
      log "Snapshotting specific tables: ${TABLES[*]}"
      TABLE_ARGS=$(printf '{"name": "--tables"},\n'; for t in "${TABLES[@]}"; do echo '{"name": "'"$t"'"},'; done)
      # Apply job with patched args
      kubectl apply -f k8s/04-job-snap.yaml
      kubectl patch job msc-snap -n "$NAMESPACE" \
        --type=json \
        -p="[{\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/args/-\", \"value\": \"--tables\"}$(for t in "${TABLES[@]}"; do echo ", {\"op\": \"add\", \"path\": \"/spec/template/spec/containers/0/args/-\", \"value\": \"$t\"}"; done)]" \
        2>/dev/null || true
    else
      kubectl apply -f k8s/04-job-snap.yaml
    fi

    log "Waiting for snapshot to complete (timeout: $SNAP_TIMEOUT)..."
    log "Tailing logs..."
    kubectl logs -f job/msc-snap -n "$NAMESPACE" &
    LOG_PID=$!

    if kubectl wait --for=condition=complete job/msc-snap \
      -n "$NAMESPACE" --timeout="$SNAP_TIMEOUT"; then
      kill $LOG_PID 2>/dev/null || true
      ok "Snapshot complete"
    else
      kill $LOG_PID 2>/dev/null || true
      fail "Snapshot failed or timed out. Check: kubectl logs job/msc-snap -n $NAMESPACE"
    fi
  fi

else
  warn "Skipping snapshot (--skip-snap)"
fi

# ── Step 4: Deploy stream + sink ──────────────────────────────────
log "Deploying msc-stream..."
kubectl apply -f k8s/05-deployment-stream.yaml

log "Waiting for msc-stream to be ready..."
kubectl rollout status deployment/msc-stream \
  -n "$NAMESPACE" --timeout=120s

ok "msc-stream running"

log "Deploying msc-sink..."
kubectl apply -f k8s/06-deployment-sink.yaml

log "Waiting for msc-sink to be ready..."
kubectl rollout status deployment/msc-sink \
  -n "$NAMESPACE" --timeout=120s

ok "msc-sink running"

# ── Step 5: Status ────────────────────────────────────────────────
echo ""
echo -e "${GREEN}══════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  mongo-stream-cdc deployed successfully${NC}"
echo -e "${GREEN}══════════════════════════════════════════════════════${NC}"
echo ""
kubectl get pods -n "$NAMESPACE"
echo ""
log "Tail logs:"
echo "  kubectl logs -f deployment/msc-stream -n $NAMESPACE"
echo "  kubectl logs -f deployment/msc-sink   -n $NAMESPACE"
echo ""
log "Re-init (oplog expired):"
echo "  ./deploy.sh --reinit"

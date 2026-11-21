#!/bin/bash
# add-collection.sh — mongo-stream-cdc
# Add a new collection to a running pipeline without full re-deployment.
#
# What this does:
#   1. Validates the new collection entry exists in the updated table config
#   2. Updates the msc-table-config ConfigMap with the new config file
#   3. Runs a targeted msc_snap job for only the new collection(s)
#   4. Rolls out msc-stream and msc-sink so they pick up the new topics
#
# Usage:
#   ./add-collection.sh --tables MyNewCollection
#   ./add-collection.sh --tables Col1 Col2 Col3
#
# Prerequisites:
#   - combined_table_config_3.json already contains the new collection(s)
#   - kubectl context is set to the correct cluster

set -euo pipefail

GREEN="\033[0;32m"; RED="\033[0;31m"; YELLOW="\033[1;33m"; BLUE="\033[0;34m"; NC="\033[0m"
log()     { echo -e "${BLUE}[msc]${NC} $*"; }
ok()      { echo -e "${GREEN}[msc] ✓${NC} $*"; }
warn()    { echo -e "${YELLOW}[msc] ⚠${NC} $*"; }
fail()    { echo -e "${RED}[msc] ✗${NC} $*"; exit 1; }
section() { echo -e "\n${GREEN}══════════════════════════════════════════${NC}"; echo -e "${GREEN}  $*${NC}"; echo -e "${GREEN}══════════════════════════════════════════${NC}"; }

KUBECTL="./kubectl"
NAMESPACE="mongo-stream-cdc"
SNAP_TIMEOUT="7200s"
TABLES=()

# ── Arg Parsing ───────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --tables)
      shift
      while [[ $# -gt 0 && ! "$1" =~ ^-- ]]; do
        TABLES+=("$1"); shift
      done
      ;;
    --namespace)
      NAMESPACE="$2"; shift 2 ;;
    --timeout)
      SNAP_TIMEOUT="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: $0 --tables CollectionName [Col2 Col3 ...]"
      echo ""
      echo "Options:"
      echo "  --tables NAME [...]   Collection(s) to add (required)"
      echo "  --namespace NS        Kubernetes namespace (default: mongo-stream-cdc)"
      echo "  --timeout DURATION    Snapshot job timeout (default: 7200s)"
      exit 0
      ;;
    *)
      fail "Unknown argument: $1. Use --help for usage." ;;
  esac
done

[[ ${#TABLES[@]} -gt 0 ]] || fail "No tables specified. Usage: $0 --tables MyCollection"

section "mongo-stream-cdc — Add Collection(s)"
log "Collections to add: ${TABLES[*]}"
log "Namespace: $NAMESPACE"
echo ""

# ── Pre-flight ────────────────────────────────────────────────────
section "Step 1: Pre-flight checks"

# Verify kubectl context
CONTEXT=$($KUBECTL config current-context)
log "kubectl context: $CONTEXT"
read -p "Continue? [y/N] " -n 1 -r; echo
[[ $REPLY =~ ^[Yy]$ ]] || fail "Aborted."

# Verify table config file exists
[[ -f "./combined_table_config_3.json" ]] || fail "combined_table_config_3.json not found in current directory"

# Verify all specified tables exist in the config file
for table in "${TABLES[@]}"; do
  if ! python3 -c "
import json, sys
data = json.load(open('./combined_table_config_3.json'))
names = [t['table_name'] for t in data]
if '$table' not in names:
    print(f'ERROR: $table not found in combined_table_config_3.json')
    print(f'Available: {names}')
    sys.exit(1)
print(f'  ✓  $table found in table config')
"; then
    fail "Table '$table' not found in combined_table_config_3.json — add it first"
  fi
done
ok "All specified tables found in config"

# Verify the namespace and pipeline are running
$KUBECTL get namespace "$NAMESPACE" &>/dev/null || fail "Namespace $NAMESPACE does not exist. Run setup.sh first."
$KUBECTL get deployment msc-stream -n "$NAMESPACE" &>/dev/null || fail "msc-stream deployment not found. Run setup.sh first."
$KUBECTL get deployment msc-sink   -n "$NAMESPACE" &>/dev/null || fail "msc-sink deployment not found. Run setup.sh first."
ok "Namespace and pipeline deployments found"

# ── Step 2: Update ConfigMap ──────────────────────────────────────
section "Step 2: Updating msc-table-config ConfigMap"

log "Applying updated combined_table_config_3.json..."
$KUBECTL create configmap msc-table-config \
  --from-file=combined_table_config_3.json=./combined_table_config_3.json \
  --namespace "$NAMESPACE" \
  --dry-run=client -o yaml | $KUBECTL apply -f -
ok "ConfigMap updated"

# ── Step 3: Snapshot new collection(s) only ───────────────────────
section "Step 3: Running snapshot for new collection(s)"

# Delete any existing snap job to allow re-run
$KUBECTL delete job msc-snap -n "$NAMESPACE" --ignore-not-found=true
log "Waiting for old snap job to be deleted..."
for i in $(seq 1 20); do
  if ! $KUBECTL get job msc-snap -n "$NAMESPACE" &>/dev/null; then
    break
  fi
  sleep 2
done

# Get current image from the running stream deployment
IMAGE=$($KUBECTL get deployment msc-stream -n "$NAMESPACE" \
  -o jsonpath='{.spec.template.spec.containers[0].image}')
log "Using image: $IMAGE"
IMAGE_REPO="${IMAGE%:*}"
IMAGE_TAG="${IMAGE##*:}"

# Get kafka bootstrap servers from the msc-config configmap
KAFKA_SERVERS=$($KUBECTL get configmap msc-config -n "$NAMESPACE" \
  -o jsonpath='{.data.config\.json}' | python3 -c "
import json, sys
cfg = json.load(sys.stdin)
print(','.join(cfg['kafka_bootstrap_servers']))
")
log "Kafka: $KAFKA_SERVERS"
KAFKA_SERVERS_HELM="${KAFKA_SERVERS//,/\\,}"

# Build --tables args for the snap job
TABLES_ARGS=""
for table in "${TABLES[@]}"; do
  TABLES_ARGS="${TABLES_ARGS}            - \"${table}\"\n"
done

log "Launching targeted snapshot job for: ${TABLES[*]}"

$KUBECTL apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: msc-snap
  namespace: ${NAMESPACE}
  labels:
    app: msc-snap
    msc-add-collection: "true"
spec:
  ttlSecondsAfterFinished: 3600
  backoffLimit: 2
  template:
    metadata:
      labels:
        app: msc-snap
    spec:
      restartPolicy: OnFailure
      serviceAccountName: msc-snap
      containers:
        - name: msc-snap
          image: ${IMAGE}
          imagePullPolicy: Always
          command: ["python", "msc_snap.py"]
          args:
            - "--config"
            - "/config/config.json"
            - "--table-config"
            - "/table-config/combined_table_config_3.json"
            - "--batch-size"
            - "500"
            - "--tables"
$(for table in "${TABLES[@]}"; do echo "            - \"${table}\""; done)
          env:
            - name: MONGO_URI
              valueFrom:
                secretKeyRef:
                  name: msc-secrets
                  key: MONGO_URI
            - name: POSTGRES_URI
              valueFrom:
                secretKeyRef:
                  name: msc-secrets
                  key: POSTGRES_URI
            - name: KAFKA_USERNAME
              valueFrom:
                secretKeyRef:
                  name: msc-secrets
                  key: KAFKA_USERNAME
            - name: KAFKA_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: msc-secrets
                  key: KAFKA_PASSWORD
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          volumeMounts:
            - name: config-vol
              mountPath: /config
            - name: table-config-vol
              mountPath: /table-config
      volumes:
        - name: config-vol
          configMap:
            name: msc-config
        - name: table-config-vol
          configMap:
            name: msc-table-config
EOF

log "Tailing snapshot logs (waiting for pod to start)..."
sleep 5
$KUBECTL logs -f job/msc-snap -n "$NAMESPACE" &
LOG_PID=$!

if $KUBECTL wait --for=condition=complete job/msc-snap -n "$NAMESPACE" --timeout="$SNAP_TIMEOUT"; then
  kill $LOG_PID 2>/dev/null || true
  ok "Snapshot complete for: ${TABLES[*]}"
else
  kill $LOG_PID 2>/dev/null || true
  fail "Snapshot failed. Check: $KUBECTL logs job/msc-snap -n $NAMESPACE"
fi

# ── Step 4: Roll out stream + sink ────────────────────────────────
section "Step 4: Rolling out stream + sink to pick up new collection(s)"

log "Rolling out msc-stream..."
$KUBECTL rollout restart deployment/msc-stream -n "$NAMESPACE"
$KUBECTL rollout status deployment/msc-stream -n "$NAMESPACE" --timeout=120s
ok "msc-stream rolled out"

log "Rolling out msc-sink..."
$KUBECTL rollout restart deployment/msc-sink -n "$NAMESPACE"
$KUBECTL rollout status deployment/msc-sink -n "$NAMESPACE" --timeout=120s
ok "msc-sink rolled out"

# ── Done ──────────────────────────────────────────────────────────
section "Done — Collection(s) Added"

log "New collection(s) active: ${TABLES[*]}"
echo ""
log "Kafka topics (auto-created on first event):"
for table in "${TABLES[@]}"; do
  echo "  msc.$(echo "$table" | tr '[:upper:]' '[:lower:]')"
done
echo ""
log "Tail logs:"
echo "  $KUBECTL logs -f deployment/msc-stream -n $NAMESPACE"
echo "  $KUBECTL logs -f deployment/msc-sink   -n $NAMESPACE"
echo ""
log "To add more collections later:"
echo "  ./add-collection.sh --tables AnotherCollection"


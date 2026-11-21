#!/bin/bash
# uninstall.sh — mongo-stream-cdc
# Tears down the full pipeline from Kubernetes.
# Kafka topics and Postgres data are NOT deleted unless explicitly confirmed.
#
# Usage:
#   ./uninstall.sh

set -euo pipefail

GREEN="\033[0;32m"; RED="\033[0;31m"; YELLOW="\033[1;33m"; BLUE="\033[0;34m"; NC="\033[0m"
log()  { echo -e "${BLUE}[msc]${NC} $*"; }
ok()   { echo -e "${GREEN}[msc] ✓${NC} $*"; }
warn() { echo -e "${YELLOW}[msc] ⚠${NC} $*"; }
fail() { echo -e "${RED}[msc] ✗${NC} $*"; exit 1; }
section() { echo -e "\n${GREEN}══════════════════════════════════════════${NC}"; echo -e "${GREEN}  $*${NC}"; echo -e "${GREEN}══════════════════════════════════════════${NC}"; }

NAMESPACE="mongo-stream-cdc"

section "mongo-stream-cdc — Uninstall"
warn "This will remove all pipeline resources from Kubernetes."
warn "Postgres data and Kafka CDC topics will NOT be deleted unless you confirm below."
echo ""

CONTEXT=$(kubectl config current-context)
log "Current kubectl context: $CONTEXT"
read -p "Continue with uninstall? [y/N] " -n 1 -r; echo
[[ $REPLY =~ ^[Yy]$ ]] || fail "Aborted."

# ── Step 1: Stop deployments ──────────────────────────────────────
section "Step 1: Stopping deployments"

log "Scaling down msc-stream..."
kubectl scale deployment/msc-stream --replicas=0 -n "$NAMESPACE" --ignore-not-found=true

log "Scaling down msc-sink..."
kubectl scale deployment/msc-sink --replicas=0 -n "$NAMESPACE" --ignore-not-found=true

log "Waiting for pods to terminate..."
kubectl wait --for=delete pod \
  -l "app in (msc-stream,msc-sink)" \
  -n "$NAMESPACE" --timeout=60s 2>/dev/null || true

ok "Deployments stopped"

# ── Step 2: Delete K8s resources ─────────────────────────────────
section "Step 2: Deleting Kubernetes resources"

log "Deleting deployments..."
kubectl delete deployment msc-stream msc-sink \
  -n "$NAMESPACE" --ignore-not-found=true

log "Deleting jobs..."
kubectl delete job msc-snap \
  -n "$NAMESPACE" --ignore-not-found=true

log "Deleting any lingering doctor/init pods..."
kubectl delete pod -l "app in (msc-doctor,msc-kafka-init)" \
  -n "$NAMESPACE" --ignore-not-found=true

log "Deleting configmaps..."
kubectl delete configmap msc-config msc-table-config msc-resume-point \
  -n "$NAMESPACE" --ignore-not-found=true

log "Deleting secrets..."
kubectl delete secret msc-secrets \
  -n "$NAMESPACE" --ignore-not-found=true

log "Deleting RBAC resources..."
kubectl delete rolebinding msc-stream-lease \
  -n "$NAMESPACE" --ignore-not-found=true
kubectl delete role msc-stream-lease \
  -n "$NAMESPACE" --ignore-not-found=true
kubectl delete serviceaccount msc-stream \
  -n "$NAMESPACE" --ignore-not-found=true

log "Deleting K8s lease (leader election)..."
kubectl delete lease msc-stream-leader \
  -n "$NAMESPACE" --ignore-not-found=true 2>/dev/null || true

ok "Kubernetes resources deleted"

# ── Step 3: Delete namespace ──────────────────────────────────────
section "Step 3: Deleting namespace"
warn "Deleting namespace: $NAMESPACE"
read -p "Delete namespace $NAMESPACE? [y/N] " -n 1 -r; echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
  kubectl delete namespace "$NAMESPACE" --ignore-not-found=true
  ok "Namespace deleted"
else
  warn "Namespace kept: $NAMESPACE"
fi

# ── Step 4: Kafka topics ──────────────────────────────────────────
section "Step 4: Kafka topics"
warn "The following Kafka topics were created by this pipeline:"
echo "  msc.resume  (resume token — safe to delete)"
echo "  msc.dlq     (dead letter queue — review before deleting)"
echo "  msc.*       (one per collection — contains unprocessed CDC events)"
echo ""
warn "Deleting Kafka topics will lose any unprocessed CDC events in flight."
echo ""
read -p "Delete ALL msc.* Kafka topics? [y/N] " -n 1 -r; echo

if [[ $REPLY =~ ^[Yy]$ ]]; then
  read -p "Kafka Bootstrap Servers (host:port): "   KAFKA_SERVERS
  read -p "Kafka Username: "                         KAFKA_USERNAME
  read -s -p "Kafka Password: "                      KAFKA_PASSWORD; echo

  log "Deleting Kafka topics via transient pod..."
  cat <<EOF | kubectl apply -f - 2>/dev/null || true
apiVersion: v1
kind: Namespace
metadata:
  name: msc-cleanup
EOF

  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: msc-kafka-cleanup
  namespace: msc-cleanup
spec:
  restartPolicy: Never
  containers:
    - name: kafka-cleanup
      image: confluentinc/cp-kafka:7.5.0
      command:
        - bash
        - -c
        - |
          echo "Listing msc.* topics..."
          kafka-topics.sh \
            --bootstrap-server $KAFKA_SERVERS \
            --command-config /tmp/client.properties \
            --list | grep "^msc\." | while read topic; do
              echo "Deleting: \$topic"
              kafka-topics.sh \
                --bootstrap-server $KAFKA_SERVERS \
                --command-config /tmp/client.properties \
                --delete --topic "\$topic" || true
          done
          echo "Done."
      volumeMounts:
        - name: client-props
          mountPath: /tmp
  initContainers:
    - name: write-props
      image: busybox
      command:
        - sh
        - -c
        - |
          cat > /tmp/client.properties <<PROPS
          security.protocol=SASL_PLAINTEXT
          sasl.mechanism=PLAIN
          sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$KAFKA_USERNAME" password="$KAFKA_PASSWORD";
          PROPS
      volumeMounts:
        - name: client-props
          mountPath: /tmp
  volumes:
    - name: client-props
      emptyDir: {}
EOF

  log "Waiting for Kafka cleanup..."
  for i in $(seq 1 30); do
    PHASE=$(kubectl get pod msc-kafka-cleanup -n msc-cleanup \
      -o jsonpath='{.status.phase}' 2>/dev/null || echo "Pending")
    if [[ "$PHASE" == "Succeeded" ]]; then
      kubectl logs pod/msc-kafka-cleanup -n msc-cleanup 2>/dev/null || true
      ok "Kafka topics deleted"
      break
    elif [[ "$PHASE" == "Failed" ]]; then
      warn "Kafka cleanup pod failed — delete topics manually"
      break
    fi
    sleep 3
  done

  kubectl delete pod msc-kafka-cleanup -n msc-cleanup --ignore-not-found=true 2>/dev/null || true
  kubectl delete namespace msc-cleanup --ignore-not-found=true 2>/dev/null || true
else
  warn "Kafka topics kept. Delete manually if needed."
fi

# ── Step 5: Postgres ──────────────────────────────────────────────
section "Step 5: Postgres data"
warn "Postgres tables created by this pipeline are NOT deleted automatically."
warn "To clean up Postgres, connect and run:"
echo "  DROP TABLE <table_name>;  -- for each replicated collection"
echo ""
warn "Or drop the entire analytics schema/database if appropriate."

# ── Done ──────────────────────────────────────────────────────────
section "Uninstall Complete"
ok "All pipeline resources removed from Kubernetes."
echo ""
log "To reinstall:"
echo "  ./setup.sh"

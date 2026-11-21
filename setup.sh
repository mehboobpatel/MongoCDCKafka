#!/bin/bash
# setup.sh — mongo-stream-cdc
# Full setup on GKE using Helm.

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

section "mongo-stream-cdc — Setup"

read -p "GCP Project ID: "                   PROJECT_ID
read -p "GKE Cluster Name: "                 CLUSTER_NAME
read -p "GKE Region (e.g. asia-south1-a): "  REGION
read -p "Image Tag (e.g. 1.0.1): "           IMAGE_TAG
read -p "Kafka Bootstrap Servers (comma-separated host:port): " KAFKA_SERVERS
read -p "Kafka Username: "                   KAFKA_USERNAME
read -s -p "Kafka Password: "                KAFKA_PASSWORD; echo
read -s -p "MongoDB URI: "                   MONGO_URI; echo
read -s -p "Postgres URI: "                  POSTGRES_URI; echo

IMAGE_REPO="asia-south1-docker.pkg.dev/${PROJECT_ID}/interaktprod/mongo-stream-cdc"
IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"

echo ""
log "Summary:"
echo "  Project:   $PROJECT_ID"
echo "  Cluster:   $CLUSTER_NAME"
echo "  Region:    $REGION"
echo "  Image:     $IMAGE"
echo "  Kafka:     $KAFKA_SERVERS"
echo "  Namespace: $NAMESPACE"
echo ""
read -p "Proceed? [y/N] " -n 1 -r; echo
[[ $REPLY =~ ^[Yy]$ ]] || fail "Aborted."

KAFKA_SERVERS_HELM="${KAFKA_SERVERS//,/\\,}"

# ── Step 1: kubectl context ───────────────────────────────────────
section "Step 1: Connecting to GKE cluster"
gcloud container clusters get-credentials "$CLUSTER_NAME" --region "$REGION" --project "$PROJECT_ID"
ok "kubectl context: $($KUBECTL config current-context)"

# ── Step 2: Build & push ──────────────────────────────────────────
section "Step 2: Building & pushing Docker image"
gcloud auth configure-docker asia-south1-docker.pkg.dev --quiet
log "Building: $IMAGE"
sudo docker build --no-cache -t "$IMAGE" .
sudo docker push "$IMAGE"
ok "Image pushed: $IMAGE"

# ── Step 3: Namespace + secrets ───────────────────────────────────
section "Step 3: Creating namespace and secrets"

$KUBECTL create namespace "$NAMESPACE" 2>/dev/null || true

$KUBECTL create secret generic msc-secrets \
  --namespace "$NAMESPACE" \
  --from-literal=MONGO_URI="$MONGO_URI" \
  --from-literal=POSTGRES_URI="$POSTGRES_URI" \
  --from-literal=KAFKA_USERNAME="$KAFKA_USERNAME" \
  --from-literal=KAFKA_PASSWORD="$KAFKA_PASSWORD" \
  --save-config 2>/dev/null || \
$KUBECTL create secret generic msc-secrets \
  --namespace "$NAMESPACE" \
  --from-literal=MONGO_URI="$MONGO_URI" \
  --from-literal=POSTGRES_URI="$POSTGRES_URI" \
  --from-literal=KAFKA_USERNAME="$KAFKA_USERNAME" \
  --from-literal=KAFKA_PASSWORD="$KAFKA_PASSWORD"

$KUBECTL create configmap msc-table-config \
  --from-file=combined_table_config_3.json=./combined_table_config_3.json \
  --namespace "$NAMESPACE" --save-config 2>/dev/null || \
$KUBECTL create configmap msc-table-config \
  --from-file=combined_table_config_3.json=./combined_table_config_3.json \
  --namespace "$NAMESPACE"

$KUBECTL create configmap msc-resume-point \
  --namespace "$NAMESPACE" \
  --from-literal=resume_point.json='{}' --save-config 2>/dev/null || true

ok "Namespace and secrets ready"

# ── Step 4: Install Helm if needed ───────────────────────────────
section "Step 4: Ensuring Helm is installed"

if ! command -v helm &>/dev/null; then
  log "Installing Helm..."
  curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
fi

ok "Helm ready"

# ── Step 5: Apply configmap via Helm and wait for it ─────────────
section "Step 5: Applying configmap via Helm (required before doctor pod)"

helm upgrade --install mongo-stream-cdc ./helm \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE_REPO" \
  --set image.tag="$IMAGE_TAG" \
  --set-string "kafka.bootstrapServers=${KAFKA_SERVERS_HELM}" \
  --set snap.enabled=false \
  --timeout=60s 2>/dev/null || true

log "Waiting for msc-config configmap to be available..."
for i in $(seq 1 20); do
  if $KUBECTL get configmap msc-config -n "$NAMESPACE" &>/dev/null; then
    ok "ConfigMap msc-config is ready"
    break
  fi
  if [[ $i -eq 20 ]]; then
    fail "Timed out waiting for msc-config configmap. Check your Helm chart."
  fi
  log "  attempt $i/20 — retrying in 3s..."
  sleep 3
done

# ── Step 6: Pre-flight doctor pod ────────────────────────────────
section "Step 6: Running pre-flight checks (msc_doctor)"
DOCTOR_POD="msc-doctor-$(date +%s)"

$KUBECTL apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${DOCTOR_POD}
  namespace: ${NAMESPACE}
  labels:
    app: msc-doctor
spec:
  restartPolicy: Never
  containers:
    - name: msc-doctor
      image: ${IMAGE}
      imagePullPolicy: Always
      command: ["python", "msc_doctor.py"]
      args: ["--config", "/config/config.json", "--table-config", "/table-config/combined_table_config_3.json"]
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

log "Waiting for doctor pod..."
sleep 5
$KUBECTL logs -f "pod/${DOCTOR_POD}" -n "$NAMESPACE" &
LOG_PID=$!

for i in $(seq 1 60); do
  PHASE=$($KUBECTL get pod "$DOCTOR_POD" -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Pending")
  if [[ "$PHASE" == "Succeeded" ]]; then
    kill $LOG_PID 2>/dev/null || true
    ok "All pre-flight checks passed"
    break
  elif [[ "$PHASE" == "Failed" ]]; then
    kill $LOG_PID 2>/dev/null || true
    $KUBECTL delete pod "$DOCTOR_POD" -n "$NAMESPACE" --ignore-not-found=true
    fail "Pre-flight checks failed. Fix above issues and re-run setup."
  fi
  sleep 5
done
$KUBECTL delete pod "$DOCTOR_POD" -n "$NAMESPACE" --ignore-not-found=true

# ── Step 7: Create Kafka topics ───────────────────────────────────
section "Step 7: Creating Kafka topics"

$KUBECTL apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: msc-kafka-init
  namespace: ${NAMESPACE}
spec:
  restartPolicy: Never
  containers:
    - name: kafka-init
      image: ${IMAGE}
      imagePullPolicy: Always
      command:
        - python3
        - -c
        - |
          import os, certifi
          from confluent_kafka.admin import AdminClient, NewTopic
          from confluent_kafka import KafkaException
          cfg = {
              "bootstrap.servers": os.environ["KAFKA_SERVERS"],
              "security.protocol": "SASL_SSL",
              "sasl.mechanism": "PLAIN",
              "sasl.username": os.environ["KAFKA_USERNAME"],
              "sasl.password": os.environ["KAFKA_PASSWORD"],
              "ssl.ca.location": certifi.where(),
              "ssl.endpoint.identification.algorithm": "",
          }
          admin = AdminClient(cfg)
          topics = [
              NewTopic("msc.resume", num_partitions=1, replication_factor=1,
                       config={"cleanup.policy": "compact", "min.cleanable.dirty.ratio": "0.01", "segment.ms": "60000"}),
              NewTopic("msc.dlq", num_partitions=1, replication_factor=1,
                       config={"retention.ms": "604800000"}),
          ]
          for topic, f in admin.create_topics(topics).items():
              try:
                  f.result(); print(f"Created: {topic}")
              except KafkaException as e:
                  print(f"{'Already exists' if 'already exists' in str(e).lower() else 'Error'}: {topic} — {e}")
      env:
        - name: KAFKA_SERVERS
          value: "${KAFKA_SERVERS}"
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
EOF

for i in $(seq 1 30); do
  PHASE=$($KUBECTL get pod msc-kafka-init -n "$NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || echo "Pending")
  [[ "$PHASE" == "Succeeded" ]] && { $KUBECTL logs pod/msc-kafka-init -n "$NAMESPACE" 2>/dev/null; ok "Kafka topics ready"; break; }
  [[ "$PHASE" == "Failed" ]]    && { warn "Kafka topic creation failed — topics may already exist"; break; }
  sleep 3
done
$KUBECTL delete pod msc-kafka-init -n "$NAMESPACE" --ignore-not-found=true

# ── Step 8: Helm deploy snap ──────────────────────────────────────
section "Step 8: Running snapshot job"

helm upgrade --install mongo-stream-cdc ./helm \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE_REPO" \
  --set image.tag="$IMAGE_TAG" \
  --set-string "kafka.bootstrapServers=${KAFKA_SERVERS_HELM}" \
  --set snap.enabled=true

log "Waiting for snapshot (timeout: $SNAP_TIMEOUT)..."
$KUBECTL logs -f job/msc-snap -n "$NAMESPACE" &
LOG_PID=$!

if $KUBECTL wait --for=condition=complete job/msc-snap -n "$NAMESPACE" --timeout="$SNAP_TIMEOUT"; then
  kill $LOG_PID 2>/dev/null || true
  ok "Snapshot complete"
else
  kill $LOG_PID 2>/dev/null || true
  fail "Snapshot failed. Check: $KUBECTL logs job/msc-snap -n $NAMESPACE"
fi

# ── Step 9: Deploy stream + sink ──────────────────────────────────
section "Step 9: Deploying stream + sink"

helm upgrade mongo-stream-cdc ./helm \
  --namespace "$NAMESPACE" \
  --set image.repository="$IMAGE_REPO" \
  --set image.tag="$IMAGE_TAG" \
  --set-string "kafka.bootstrapServers=${KAFKA_SERVERS_HELM}" \
  --set snap.enabled=false \
  --wait --timeout=120s

ok "Stream + sink deployed"

# ── Done ──────────────────────────────────────────────────────────
section "Setup Complete"
$KUBECTL get pods -n "$NAMESPACE"
echo ""
log "Tail logs:"
echo "  $KUBECTL logs -f deployment/msc-stream -n $NAMESPACE"
echo "  $KUBECTL logs -f deployment/msc-sink   -n $NAMESPACE"
echo ""
log "Uninstall: ./uninstall.sh"

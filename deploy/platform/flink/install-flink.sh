#!/usr/bin/env bash
# Install Flink Kubernetes Operator (CRDs + controller), then apply the
# kind-sized FlinkDeployment + RBAC under this directory.
#
# Usage:
#   ./install-flink.sh
#   FLINK_OPERATOR_VERSION=1.8.0 ./install-flink.sh
#
# Prerequisites: kubectl, helm, a reachable cluster (e.g. kind).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- configurable knobs -------------------------------------------------------
NAMESPACE="${NAMESPACE:-fluxmeter}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-flink-operator}"
# Chart repo path is versioned on downloads.apache.org; bump when leaving WIP.
FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-1.8.0}"
FLINK_OPERATOR_RELEASE="${FLINK_OPERATOR_RELEASE:-flink-kubernetes-operator}"
FLINK_OPERATOR_REPO_NAME="${FLINK_OPERATOR_REPO_NAME:-flink-operator-repo}"
FLINK_OPERATOR_REPO_URL="${FLINK_OPERATOR_REPO_URL:-https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/}"
# cert-manager is required by the Operator webhook; skip with SKIP_CERT_MANAGER=1
# if your cluster already has it.
SKIP_CERT_MANAGER="${SKIP_CERT_MANAGER:-0}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.14.5}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"

log()  { printf '==> %s\n' "$*"; }
die()  { printf 'error: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }

# --- preflight ----------------------------------------------------------------
need kubectl
need helm
kubectl cluster-info >/dev/null 2>&1 || die "kubectl cannot reach a cluster"

log "Target namespace: ${NAMESPACE}"
log "Operator namespace: ${OPERATOR_NAMESPACE}"
log "Operator chart version: ${FLINK_OPERATOR_VERSION}"

# --- 0. Ensure app namespace exists -------------------------------------------
kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || {
  log "Creating namespace ${NAMESPACE}"
  kubectl create namespace "${NAMESPACE}"
}

# --- 1. cert-manager (Operator webhook dependency) ----------------------------
# FlinkDeployment apply will fail with "no matches for kind" if CRDs are absent;
# webhook admission will fail later if cert-manager is missing.
if [[ "${SKIP_CERT_MANAGER}" != "1" ]]; then
  if kubectl get crd certificates.cert-manager.io >/dev/null 2>&1; then
    log "cert-manager CRDs already present — skipping install"
  else
    log "Installing cert-manager ${CERT_MANAGER_VERSION}"
    kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
    log "Waiting for cert-manager webhook to become Available"
    kubectl -n cert-manager wait --for=condition=Available deploy/cert-manager-webhook --timeout="${WAIT_TIMEOUT}"
  fi
else
  log "SKIP_CERT_MANAGER=1 — assuming cert-manager is already installed"
fi

# --- 2. Flink Kubernetes Operator (installs FlinkDeployment CRDs) -------------
log "Adding / updating Helm repo ${FLINK_OPERATOR_REPO_NAME}"
helm repo add "${FLINK_OPERATOR_REPO_NAME}" "${FLINK_OPERATOR_REPO_URL}" >/dev/null 2>&1 || true
helm repo update "${FLINK_OPERATOR_REPO_NAME}" >/dev/null

log "Installing / upgrading Flink Operator release ${FLINK_OPERATOR_RELEASE}"
helm upgrade --install "${FLINK_OPERATOR_RELEASE}" \
  "${FLINK_OPERATOR_REPO_NAME}/flink-kubernetes-operator" \
  --namespace "${OPERATOR_NAMESPACE}" \
  --create-namespace \
  --wait \
  --timeout "${WAIT_TIMEOUT}"

log "Waiting for Operator deployment to finish rollout"
kubectl -n "${OPERATOR_NAMESPACE}" rollout status \
  "deploy/${FLINK_OPERATOR_RELEASE}" \
  --timeout="${WAIT_TIMEOUT}"

# Fail fast with a clear message if CRDs never appeared (bad chart / partial install).
if ! kubectl get crd flinkdeployments.flink.apache.org >/dev/null 2>&1; then
  die "CRD flinkdeployments.flink.apache.org not found after Operator install — check Helm release logs"
fi
log "CRD flinkdeployments.flink.apache.org is present"

# --- 3. RBAC + kind FlinkDeployment -------------------------------------------
log "Applying kustomize overlay: ${SCRIPT_DIR}"
kubectl apply -k "${SCRIPT_DIR}"

# --- 4. Smoke status ----------------------------------------------------------
log "Current Flink resources in ${NAMESPACE}"
kubectl -n "${NAMESPACE}" get flinkdeployment,pods,svc 2>/dev/null || \
  kubectl -n "${NAMESPACE}" get pods,svc

cat <<EOF

Done.

Next checks:
  kubectl -n ${NAMESPACE} get flinkdeployment
  kubectl -n ${NAMESPACE} get pods
  kubectl -n ${NAMESPACE} get svc | grep rest

UI (once fluxmeter-rest exists):
  kubectl -n ${NAMESPACE} port-forward svc/fluxmeter-rest 8081:8081
  open http://localhost:8081
EOF
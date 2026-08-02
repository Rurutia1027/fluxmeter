#!/usr/bin/env bash
# Install Flink Kubernetes Operator (CRDs + controller), then apply the
# kind-sized FlinkDeployment + RBAC under deploy/platform/flink/.
#
# Usage (from repo root or any cwd):
#   ./deploy/platform/flink/install.sh
#
# Prerequisites: kubectl, helm, a reachable cluster (e.g. kind).
# Order matters: Operator/CRDs must exist before FlinkDeployment is applied.
#
# Optional env overrides:
#   OPERATOR_VERSION   Flink Operator release (default: 1.15.0)
#   WEBHOOK_CREATE     true|false — Operator validating webhook (default: true)
#   INSTALL_CERT_MANAGER  true|false — install cert-manager when webhook is on
#                         and CRDs are missing (default: true)
#   FLUXMETER_NS / OPERATOR_NS / WAIT_TIMEOUT

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- Tunables (override via env) ---
FLUXMETER_NS="${FLUXMETER_NS:-fluxmeter}"
OPERATOR_NS="${OPERATOR_NS:-flink-operator}"
OPERATOR_RELEASE="${OPERATOR_RELEASE:-flink-kubernetes-operator}"
# Apache hosts one chart index per Operator version; old pins (e.g. 1.8.0) 404.
OPERATOR_VERSION="${OPERATOR_VERSION:-1.15.0}"
HELM_REPO_NAME="${HELM_REPO_NAME:-flink-operator-repo}"
HELM_REPO_URL="${HELM_REPO_URL:-https://downloads.apache.org/flink/flink-kubernetes-operator-${OPERATOR_VERSION}/}"
# Fallback when downloads.apache.org lags / mirrors drop a release.
HELM_REPO_URL_FALLBACK="${HELM_REPO_URL_FALLBACK:-https://archive.apache.org/dist/flink/flink-kubernetes-operator-${OPERATOR_VERSION}/}"
HELM_CHART="${HELM_CHART:-${HELM_REPO_NAME}/flink-kubernetes-operator}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-300s}"
WEBHOOK_CREATE="${WEBHOOK_CREATE:-true}"
INSTALL_CERT_MANAGER="${INSTALL_CERT_MANAGER:-true}"
CERT_MANAGER_MANIFEST="${CERT_MANAGER_MANIFEST:-https://github.com/jetstack/cert-manager/releases/download/v1.18.2/cert-manager.yaml}"

log() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

need_cmd kubectl
need_cmd helm

# Ensure we can talk to a cluster before doing anything expensive.
kubectl cluster-info >/dev/null || die "kubectl cannot reach a cluster"

log "Ensuring namespace ${FLUXMETER_NS}"
kubectl get ns "${FLUXMETER_NS}" >/dev/null 2>&1 \
  || kubectl create namespace "${FLUXMETER_NS}"

# ---------------------------------------------------------------------------
# 0) cert-manager — required when Operator webhook is enabled
# ---------------------------------------------------------------------------
if [[ "${WEBHOOK_CREATE}" == "true" ]]; then
  if kubectl get crd certificates.cert-manager.io >/dev/null 2>&1; then
    log "cert-manager CRDs already present"
  elif [[ "${INSTALL_CERT_MANAGER}" == "true" ]]; then
    log "Installing cert-manager (required for Operator webhook)"
    kubectl apply -f "${CERT_MANAGER_MANIFEST}"
    log "Waiting for cert-manager deployments"
    kubectl -n cert-manager rollout status deploy/cert-manager --timeout="${WAIT_TIMEOUT}"
    kubectl -n cert-manager rollout status deploy/cert-manager-webhook --timeout="${WAIT_TIMEOUT}"
    kubectl -n cert-manager rollout status deploy/cert-manager-cainjector --timeout="${WAIT_TIMEOUT}"
  else
    die "webhook.create=true but cert-manager is missing; set INSTALL_CERT_MANAGER=true or WEBHOOK_CREATE=false"
  fi
fi

# ---------------------------------------------------------------------------
# 1) Flink Kubernetes Operator — installs FlinkDeployment CRDs + controller
# ---------------------------------------------------------------------------
add_helm_repo() {
  local url="$1"
  log "Adding Helm repo ${HELM_REPO_NAME} -> ${url}"
  if helm repo list 2>/dev/null | awk '{print $1}' | grep -qx "${HELM_REPO_NAME}"; then
    helm repo remove "${HELM_REPO_NAME}" >/dev/null
  fi
  helm repo add "${HELM_REPO_NAME}" "${url}"
}

log "Resolving Operator Helm chart (version ${OPERATOR_VERSION})"
if ! add_helm_repo "${HELM_REPO_URL}"; then
  log "Primary chart URL failed; trying archive.apache.org fallback"
  add_helm_repo "${HELM_REPO_URL_FALLBACK}" \
    || die "cannot reach Operator chart at ${HELM_REPO_URL} or ${HELM_REPO_URL_FALLBACK}"
fi
helm repo update "${HELM_REPO_NAME}"

HELM_EXTRA_ARGS=()
if [[ "${WEBHOOK_CREATE}" != "true" ]]; then
  HELM_EXTRA_ARGS+=(--set webhook.create=false)
  log "Installing Operator with webhook.create=false"
fi

log "Installing/upgrading Operator release ${OPERATOR_RELEASE} in ${OPERATOR_NS}"
helm upgrade --install "${OPERATOR_RELEASE}" "${HELM_CHART}" \
  --namespace "${OPERATOR_NS}" \
  --create-namespace \
  --wait \
  --timeout "${WAIT_TIMEOUT}" \
  "${HELM_EXTRA_ARGS[@]+"${HELM_EXTRA_ARGS[@]}"}"

# Webhook + deployment must be Ready before any FlinkDeployment apply.
log "Waiting for Operator deployment to become Ready"
kubectl -n "${OPERATOR_NS}" rollout status \
  "deploy/${OPERATOR_RELEASE}" \
  --timeout="${WAIT_TIMEOUT}"

log "Verifying FlinkDeployment CRD is registered"
kubectl get crd flinkdeployments.flink.apache.org >/dev/null \
  || die "CRD flinkdeployments.flink.apache.org not found after Operator install"

# ---------------------------------------------------------------------------
# 2) RBAC (SA/Role/RoleBinding) + kind FlinkDeployment (1 JM + 1 TM)
# ---------------------------------------------------------------------------
log "Applying Flink RBAC + FlinkDeployment from ${SCRIPT_DIR}"
kubectl apply -k "${SCRIPT_DIR}"

log "Current FlinkDeployment status"
kubectl -n "${FLUXMETER_NS}" get flinkdeployment || true
kubectl -n "${FLUXMETER_NS}" get pods -l app.kubernetes.io/component=flink 2>/dev/null || \
  kubectl -n "${FLUXMETER_NS}" get pods

log "Done."
log "REST UI (when Service exists): kubectl -n ${FLUXMETER_NS} port-forward svc/fluxmeter-rest 8081:8081"

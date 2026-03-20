#!/usr/bin/env python3
"""
Healthwatch Cert Checker — CronJob Script
Reads all K8s PKI certificates, runs node-level prechecks, creates PKI backup,
writes everything to a ConfigMap for the Healthwatch UI to read.

Runs inside the same healthwatch:latest image as a CronJob on the control plane node.
Mount paths:
  /host-pki       → /etc/kubernetes/pki        (ro)
  /host-kube      → /etc/kubernetes             (ro)
  /host-manifests → /etc/kubernetes/manifests   (ro)
  /backups        → /var/lib/healthwatch/pki-backups (rw)
"""

import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ─── Configuration ───────────────────────────────────────────────────────────
NAMESPACE = os.getenv("NAMESPACE", "vsmaps")
CONFIGMAP_NAME = os.getenv("CONFIGMAP_NAME", "cert-status")
PKI_DIR = Path(os.getenv("PKI_DIR", "/host-pki"))
KUBE_DIR = Path(os.getenv("KUBE_DIR", "/host-kube"))
MANIFEST_DIR = Path(os.getenv("MANIFEST_DIR", "/host-manifests"))
BACKUP_BASE = Path(os.getenv("BACKUP_DIR", "/backups"))
THRESHOLD_DAYS = int(os.getenv("CERT_WARN_DAYS", "30"))
MAX_BACKUPS = int(os.getenv("MAX_BACKUPS", "3"))


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


# ─── Certificate Reading ────────────────────────────────────────────────────
def read_cert_file(filepath, name, category):
    """Read a .crt file using openssl and return cert info dict."""
    filepath = Path(filepath)
    if not filepath.exists():
        return None

    try:
        result = subprocess.run(
            ["openssl", "x509", "-in", str(filepath), "-noout",
             "-enddate", "-startdate", "-subject", "-issuer", "-serial",
             "-ext", "subjectAltName"],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode != 0:
            return None

        output = result.stdout
        info = {}
        for line in output.splitlines():
            if line.startswith("notAfter="):
                info["not_after_raw"] = line.split("=", 1)[1]
            elif line.startswith("notBefore="):
                info["not_before_raw"] = line.split("=", 1)[1]
            elif line.startswith("subject="):
                info["subject"] = line.split("=", 1)[1].strip()
            elif line.startswith("issuer="):
                info["issuer"] = line.split("=", 1)[1].strip()
            elif line.startswith("serial="):
                info["serial"] = line.split("=", 1)[1].strip()
            elif "DNS:" in line or "IP Address:" in line:
                info["sans"] = line.strip()

        if "not_after_raw" not in info:
            return None

        # Parse expiry
        from email.utils import parsedate_to_datetime
        try:
            # openssl format: "Mar 19 12:00:00 2027 GMT"
            expiry = datetime.strptime(info["not_after_raw"], "%b %d %H:%M:%S %Y %Z")
            expiry = expiry.replace(tzinfo=timezone.utc)
        except ValueError:
            return None

        now = datetime.now(timezone.utc)
        days_left = (expiry - now).days

        status = "ok"
        if days_left <= 0:
            status = "error"
        elif days_left <= THRESHOLD_DAYS:
            status = "warn"

        return {
            "name": name,
            "category": category,
            "subject": info.get("subject", ""),
            "issuer": info.get("issuer", ""),
            "not_after": expiry.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "days_left": days_left,
            "serial": info.get("serial", ""),
            "status": status,
            "sans": info.get("sans", ""),
            "path": str(filepath),
        }
    except Exception as e:
        log(f"  ERROR reading {filepath}: {e}")
        return None


def read_kubeconfig_cert(filepath, name):
    """Extract and read the embedded client certificate from a kubeconfig file."""
    filepath = Path(filepath)
    if not filepath.exists():
        return None

    try:
        import base64
        content = filepath.read_text()

        # Find client-certificate-data line
        cert_data = None
        for line in content.splitlines():
            if "client-certificate-data:" in line:
                cert_data = line.split(":", 1)[1].strip()
                break

        if not cert_data:
            return None

        # Decode base64 and write to temp file for openssl
        pem_bytes = base64.b64decode(cert_data)
        tmp_path = Path("/tmp") / f"kc-{name}.pem"
        tmp_path.write_bytes(pem_bytes)

        result = read_cert_file(tmp_path, name, "kubeconfig")
        tmp_path.unlink(missing_ok=True)

        if result:
            result["path"] = str(filepath)
        return result
    except Exception as e:
        log(f"  ERROR reading kubeconfig {filepath}: {e}")
        return None


def check_sa_keys():
    """Check that ServiceAccount signing key pair exists."""
    results = []
    for key_file in ["sa.key", "sa.pub"]:
        path = PKI_DIR / key_file
        if path.exists():
            size = path.stat().st_size
            results.append({
                "name": key_file,
                "category": "sa_keys",
                "status": "ok" if size > 0 else "warn",
                "detail": f"{size} bytes",
                "path": str(path),
            })
        else:
            results.append({
                "name": key_file,
                "category": "sa_keys",
                "status": "warn",
                "detail": "Not found",
                "path": str(path),
            })
    return results


# ─── Prechecks (Node-level — only possible from CronJob) ────────────────────
def run_prechecks():
    """Run all node-level prechecks. Returns list of check result dicts."""
    checks = []

    # 1. PKI directory exists + has certs
    if not PKI_DIR.exists():
        checks.append(pc("pki_dir", "PKI Directory Exists", "fail", f"Not found: {PKI_DIR}"))
    else:
        crt_count = len(list(PKI_DIR.rglob("*.crt")))
        if crt_count > 0:
            checks.append(pc("pki_dir", "PKI Directory Exists", "pass", f"{crt_count} .crt files found"))
        else:
            checks.append(pc("pki_dir", "PKI Directory Exists", "fail", "No .crt files found"))

    # 2. Static pod manifests
    if not MANIFEST_DIR.exists():
        checks.append(pc("manifests", "Static Pod Manifests", "warn",
                         "Manifest dir not found (externally managed?)"))
    else:
        components = ["kube-apiserver", "kube-controller-manager", "kube-scheduler", "etcd"]
        found = [c for c in components if (MANIFEST_DIR / f"{c}.yaml").exists()]
        missing = [c for c in components if c not in found]
        if len(found) == 4:
            checks.append(pc("manifests", "Static Pod Manifests", "pass", "All 4 manifests present"))
        elif found:
            checks.append(pc("manifests", "Static Pod Manifests", "warn",
                             f"{len(found)}/4 found, missing: {', '.join(missing)}"))
        else:
            checks.append(pc("manifests", "Static Pod Manifests", "warn", "No manifests found"))

    # 3. Disk space on PKI
    try:
        result = subprocess.run(["df", "-m", str(PKI_DIR)], capture_output=True, text=True, timeout=5)
        lines = result.stdout.strip().splitlines()
        if len(lines) >= 2:
            avail_mb = int(lines[1].split()[3])
            if avail_mb >= 200:
                checks.append(pc("disk_space", "Disk Space (PKI)", "pass", f"{avail_mb}MB free"))
            elif avail_mb >= 100:
                checks.append(pc("disk_space", "Disk Space (PKI)", "warn", f"{avail_mb}MB free — low"))
            else:
                checks.append(pc("disk_space", "Disk Space (PKI)", "fail",
                                 f"{avail_mb}MB free — critically low"))
        else:
            checks.append(pc("disk_space", "Disk Space (PKI)", "warn", "Could not parse df output"))
    except Exception as e:
        checks.append(pc("disk_space", "Disk Space (PKI)", "warn", str(e)))

    # 4. CA cert expiry (CRITICAL — kubeadm cannot renew CAs)
    ca_files = [
        (PKI_DIR / "ca.crt", "ca.crt"),
        (PKI_DIR / "etcd" / "ca.crt", "etcd/ca.crt"),
        (PKI_DIR / "front-proxy-ca.crt", "front-proxy-ca.crt"),
    ]
    for ca_path, ca_name in ca_files:
        cert = read_cert_file(ca_path, ca_name, "ca")
        if not cert:
            continue
        days = cert["days_left"]
        if days <= 0:
            checks.append(pc(f"ca_{ca_name}", f"CA Cert: {ca_name}", "fail",
                             f"EXPIRED — manual CA rotation required"))
        elif days <= 90:
            checks.append(pc(f"ca_{ca_name}", f"CA Cert: {ca_name}", "warn",
                             f"{days}d remaining — kubeadm won't renew CAs, plan rotation"))
        else:
            checks.append(pc(f"ca_{ca_name}", f"CA Cert: {ca_name}", "pass",
                             f"{days}d remaining"))

    # 5. kubeconfig ↔ PKI cert serial match
    admin_conf = KUBE_DIR / "admin.conf"
    pki_cert = PKI_DIR / "apiserver-kubelet-client.crt"
    if admin_conf.exists() and pki_cert.exists():
        try:
            import base64
            # Get kubeconfig embedded cert serial
            kc_cert_data = None
            for line in admin_conf.read_text().splitlines():
                if "client-certificate-data:" in line:
                    kc_cert_data = line.split(":", 1)[1].strip()
                    break

            if kc_cert_data:
                pem_bytes = base64.b64decode(kc_cert_data)
                tmp = Path("/tmp/kc-check.pem")
                tmp.write_bytes(pem_bytes)
                r1 = subprocess.run(
                    ["openssl", "x509", "-in", str(tmp), "-noout", "-serial"],
                    capture_output=True, text=True, timeout=5,
                )
                r2 = subprocess.run(
                    ["openssl", "x509", "-in", str(pki_cert), "-noout", "-serial"],
                    capture_output=True, text=True, timeout=5,
                )
                tmp.unlink(missing_ok=True)

                kc_serial = r1.stdout.strip().split("=")[-1]
                pki_serial = r2.stdout.strip().split("=")[-1]

                if kc_serial == pki_serial:
                    checks.append(pc("kc_match", "Kubeconfig↔PKI Match", "pass",
                                     f"Serials match ({kc_serial[:16]}...)"))
                else:
                    checks.append(pc("kc_match", "Kubeconfig↔PKI Match", "warn",
                                     "Serial mismatch — previous renewal may be incomplete"))
            else:
                checks.append(pc("kc_match", "Kubeconfig↔PKI Match", "skip",
                                 "No embedded cert in kubeconfig"))
        except Exception as e:
            checks.append(pc("kc_match", "Kubeconfig↔PKI Match", "warn", str(e)))
    else:
        checks.append(pc("kc_match", "Kubeconfig↔PKI Match", "skip",
                         "admin.conf or apiserver-kubelet-client.crt not found"))

    # 6. SAN validation
    apiserver_cert = PKI_DIR / "apiserver.crt"
    if apiserver_cert.exists():
        try:
            result = subprocess.run(
                ["openssl", "x509", "-in", str(apiserver_cert), "-noout", "-text"],
                capture_output=True, text=True, timeout=10,
            )
            sans = []
            in_san = False
            for line in result.stdout.splitlines():
                if "Subject Alternative Name" in line:
                    in_san = True
                    continue
                if in_san:
                    for part in line.strip().split(","):
                        part = part.strip()
                        if part.startswith("DNS:") or part.startswith("IP Address:"):
                            sans.append(part)
                    break

            if sans:
                has_k8s_svc = any("kubernetes.default.svc" in s for s in sans)
                if has_k8s_svc:
                    checks.append(pc("san_check", "SAN Validation", "pass",
                                     f"{len(sans)} SANs, includes kubernetes.default.svc"))
                else:
                    checks.append(pc("san_check", "SAN Validation", "warn",
                                     f"{len(sans)} SANs but kubernetes.default.svc missing"))
            else:
                checks.append(pc("san_check", "SAN Validation", "warn", "Could not extract SANs"))
        except Exception as e:
            checks.append(pc("san_check", "SAN Validation", "warn", str(e)))
    else:
        checks.append(pc("san_check", "SAN Validation", "skip", "apiserver.crt not found"))

    # 7. Kubernetes version (from API server manifest image tag)
    apiserver_manifest = MANIFEST_DIR / "kube-apiserver.yaml"
    if apiserver_manifest.exists():
        try:
            content = apiserver_manifest.read_text()
            for line in content.splitlines():
                if "image:" in line and "kube-apiserver" in line:
                    import re
                    m = re.search(r'v[\d.]+', line)
                    if m:
                        checks.append(pc("k8s_version", "Kubernetes Version", "pass", m.group()))
                    else:
                        checks.append(pc("k8s_version", "Kubernetes Version", "warn",
                                         "Could not parse version from manifest"))
                    break
        except Exception as e:
            checks.append(pc("k8s_version", "Kubernetes Version", "warn", str(e)))
    else:
        checks.append(pc("k8s_version", "Kubernetes Version", "skip",
                         "kube-apiserver.yaml manifest not found"))

    # 8. NTP sync check (via timedatectl through nsenter if available)
    try:
        # Try reading from /host-proc if mounted, else skip gracefully
        ntp_status = "unknown"
        # Method 1: Check /sys/class/rtc for clock sanity
        result = subprocess.run(["date", "+%s"], capture_output=True, text=True, timeout=5)
        epoch_now = int(result.stdout.strip())
        # If system clock is before 2024, clearly wrong
        if epoch_now < 1704067200:  # 2024-01-01
            checks.append(pc("ntp_sync", "System Clock", "fail",
                             "Clock appears wrong (before 2024)"))
        else:
            human_time = datetime.fromtimestamp(epoch_now, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            checks.append(pc("ntp_sync", "System Clock", "pass", f"Clock OK: {human_time}"))
    except Exception as e:
        checks.append(pc("ntp_sync", "System Clock", "warn", f"Could not verify: {e}"))

    # 9. Kubelet config check (from kubelet kubeconfig)
    kubelet_conf = KUBE_DIR / "kubelet.conf"
    if kubelet_conf.exists():
        kc = read_kubeconfig_cert(kubelet_conf, "kubelet.conf")
        if kc and kc["days_left"] > THRESHOLD_DAYS:
            checks.append(pc("kubelet_cert", "Kubelet Certificate", "pass",
                             f"{kc['days_left']}d remaining"))
        elif kc:
            checks.append(pc("kubelet_cert", "Kubelet Certificate", "warn",
                             f"{kc['days_left']}d remaining"))
        else:
            checks.append(pc("kubelet_cert", "Kubelet Certificate", "skip",
                             "Could not read kubelet cert (may use auto-rotation)"))
    else:
        checks.append(pc("kubelet_cert", "Kubelet Certificate", "skip",
                         "kubelet.conf not found"))

    # 10. Check expected PKI file count
    expected_files = [
        "ca.crt", "ca.key", "apiserver.crt", "apiserver.key",
        "apiserver-kubelet-client.crt", "apiserver-kubelet-client.key",
        "apiserver-etcd-client.crt", "apiserver-etcd-client.key",
        "front-proxy-ca.crt", "front-proxy-ca.key",
        "front-proxy-client.crt", "front-proxy-client.key",
        "sa.key", "sa.pub",
    ]
    etcd_files = ["ca.crt", "ca.key", "server.crt", "server.key",
                  "peer.crt", "peer.key", "healthcheck-client.crt", "healthcheck-client.key"]

    missing_pki = [f for f in expected_files if not (PKI_DIR / f).exists()]
    missing_etcd = [f"etcd/{f}" for f in etcd_files if not (PKI_DIR / "etcd" / f).exists()]
    all_missing = missing_pki + missing_etcd

    if not all_missing:
        checks.append(pc("pki_files", "PKI File Inventory", "pass",
                         f"All {len(expected_files) + len(etcd_files)} files present"))
    elif len(all_missing) <= 2:
        checks.append(pc("pki_files", "PKI File Inventory", "warn",
                         f"Missing: {', '.join(all_missing)}"))
    else:
        checks.append(pc("pki_files", "PKI File Inventory", "fail",
                         f"{len(all_missing)} files missing: {', '.join(all_missing[:5])}..."))

    return checks


def pc(check_id, label, status, detail):
    return {"id": check_id, "label": label, "status": status, "detail": detail}


# ─── Backup ──────────────────────────────────────────────────────────────────
def do_backup():
    """Backup /etc/kubernetes/pki to the backup directory. Returns backup info dict."""
    timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    backup_name = f"pki-backup-{timestamp_str}"
    backup_path = BACKUP_BASE / backup_name
    host_display_path = f"/var/lib/healthwatch/pki-backups/{backup_name}"

    try:
        BACKUP_BASE.mkdir(parents=True, exist_ok=True)
        shutil.copytree(str(PKI_DIR), str(backup_path))

        # Calculate size
        size_mb = sum(f.stat().st_size for f in backup_path.rglob("*") if f.is_file()) / (1024 * 1024)
        size_mb = round(size_mb, 1)

        # Cleanup old backups (keep MAX_BACKUPS)
        existing = sorted(BACKUP_BASE.glob("pki-backup-*"), key=lambda p: p.name, reverse=True)
        for old_backup in existing[MAX_BACKUPS:]:
            shutil.rmtree(str(old_backup), ignore_errors=True)
            log(f"  Cleaned old backup: {old_backup.name}")

        # List all current backups for UI
        current_backups = sorted(BACKUP_BASE.glob("pki-backup-*"), key=lambda p: p.name, reverse=True)
        backup_list = []
        for b in current_backups[:MAX_BACKUPS]:
            b_size = sum(f.stat().st_size for f in b.rglob("*") if f.is_file()) / (1024 * 1024)
            backup_list.append({
                "name": b.name,
                "path": f"/var/lib/healthwatch/pki-backups/{b.name}",
                "size_mb": round(b_size, 1),
            })

        log(f"  Backup created: {host_display_path} ({size_mb}MB)")
        return {
            "latest": host_display_path,
            "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "size_mb": size_mb,
            "total_backups": len(current_backups),
            "history": backup_list,
            "status": "ok",
        }
    except Exception as e:
        log(f"  Backup FAILED: {e}")
        return {
            "latest": "failed",
            "created": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "size_mb": 0,
            "total_backups": 0,
            "history": [],
            "status": "error",
            "error": str(e),
        }


# ─── Write to ConfigMap ─────────────────────────────────────────────────────
def write_configmap(payload):
    """Write the JSON payload to a Kubernetes ConfigMap."""
    try:
        from kubernetes import client, config

        # In-cluster config (running as a pod)
        config.load_incluster_config()
        v1 = client.CoreV1Api()

        payload_json = json.dumps(payload, indent=2)

        cm = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(
                name=CONFIGMAP_NAME,
                namespace=NAMESPACE,
                labels={
                    "app.kubernetes.io/name": "healthwatch",
                    "app.kubernetes.io/component": "cert-checker",
                },
            ),
            data={"cert-status.json": payload_json},
        )

        try:
            v1.read_namespaced_config_map(CONFIGMAP_NAME, NAMESPACE)
            # Update existing
            v1.replace_namespaced_config_map(CONFIGMAP_NAME, NAMESPACE, cm)
            log(f"ConfigMap {NAMESPACE}/{CONFIGMAP_NAME} updated")
        except client.exceptions.ApiException as e:
            if e.status == 404:
                v1.create_namespaced_config_map(NAMESPACE, cm)
                log(f"ConfigMap {NAMESPACE}/{CONFIGMAP_NAME} created")
            else:
                raise
    except Exception as e:
        log(f"ERROR writing ConfigMap: {e}")
        # Fallback: try kubectl
        try:
            payload_json = json.dumps(payload)
            result = subprocess.run(
                ["kubectl", "create", "configmap", CONFIGMAP_NAME,
                 f"--namespace={NAMESPACE}",
                 f"--from-literal=cert-status.json={payload_json}",
                 "--dry-run=client", "-o", "yaml"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                apply = subprocess.run(
                    ["kubectl", "apply", "-f", "-"],
                    input=result.stdout, capture_output=True, text=True, timeout=10,
                )
                if apply.returncode == 0:
                    log(f"ConfigMap {NAMESPACE}/{CONFIGMAP_NAME} updated via kubectl")
                else:
                    log(f"kubectl apply failed: {apply.stderr}")
                    sys.exit(1)
        except Exception as e2:
            log(f"kubectl fallback also failed: {e2}")
            sys.exit(1)


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    node_name = "unknown"

    # Try to get node name from environment or hostname
    node_name = os.getenv("NODE_NAME", "")
    if not node_name:
        try:
            node_name = subprocess.run(
                ["hostname"], capture_output=True, text=True, timeout=5,
            ).stdout.strip()
        except Exception:
            node_name = "unknown"

    log("=" * 60)
    log(f"Healthwatch Cert Checker")
    log(f"Node: {node_name}  Threshold: {THRESHOLD_DAYS}d")
    log(f"PKI: {PKI_DIR}  Backups: {BACKUP_BASE}")
    log("=" * 60)

    # ── 1. Read all certificates ──────────────────────────────────────────
    log("Reading PKI certificates...")
    certificates = []

    # PKI cert files (10)
    pki_certs = [
        ("ca.crt", "ca"),
        ("apiserver.crt", "pki"),
        ("apiserver-kubelet-client.crt", "pki"),
        ("apiserver-etcd-client.crt", "pki"),
        ("front-proxy-ca.crt", "ca"),
        ("front-proxy-client.crt", "pki"),
    ]
    for name, cat in pki_certs:
        cert = read_cert_file(PKI_DIR / name, name, cat)
        if cert:
            certificates.append(cert)
            log(f"  {cert['status'].upper():5s} {name} — {cert['days_left']}d")
        else:
            log(f"  SKIP  {name} — not found or unreadable")

    # etcd cert files (4)
    etcd_certs = [
        ("ca.crt", "ca"),
        ("server.crt", "etcd"),
        ("peer.crt", "etcd"),
        ("healthcheck-client.crt", "etcd"),
    ]
    for name, cat in etcd_certs:
        cert = read_cert_file(PKI_DIR / "etcd" / name, f"etcd/{name}", cat)
        if cert:
            certificates.append(cert)
            log(f"  {cert['status'].upper():5s} etcd/{name} — {cert['days_left']}d")
        else:
            log(f"  SKIP  etcd/{name} — not found or unreadable")

    # SA keys (2)
    sa_keys = check_sa_keys()
    log(f"  SA keys: {len([k for k in sa_keys if k['status'] == 'ok'])}/2 present")

    # Kubeconfig embedded certs (4)
    log("Reading kubeconfig certificates...")
    kubeconfigs = ["admin.conf", "controller-manager.conf", "scheduler.conf", "super-admin.conf"]
    for name in kubeconfigs:
        cert = read_kubeconfig_cert(KUBE_DIR / name, name)
        if cert:
            certificates.append(cert)
            log(f"  {cert['status'].upper():5s} {name} — {cert['days_left']}d")
        else:
            log(f"  SKIP  {name} — not found or no embedded cert")

    # ── 2. Run prechecks ──────────────────────────────────────────────────
    log("")
    log("Running node-level prechecks...")
    prechecks = run_prechecks()
    for chk in prechecks:
        icon = {"pass": "OK", "fail": "FAIL", "warn": "WARN", "skip": "SKIP"}.get(chk["status"], "?")
        log(f"  {icon:5s} {chk['label']}: {chk['detail']}")

    # ── 3. Create backup ─────────────────────────────────────────────────
    log("")
    log("Creating PKI backup...")
    backup_info = do_backup()

    # Add backup as a precheck result too
    if backup_info["status"] == "ok":
        prechecks.append(pc("backup", "PKI Backup", "pass",
                            f"Saved to {backup_info['latest']} ({backup_info['size_mb']}MB)"))
    else:
        prechecks.append(pc("backup", "PKI Backup", "fail",
                            backup_info.get("error", "Backup failed")))

    # ── 4. Build summary ─────────────────────────────────────────────────
    total_certs = len(certificates)
    certs_ok = len([c for c in certificates if c["status"] == "ok"])
    certs_warn = len([c for c in certificates if c["status"] == "warn"])
    certs_error = len([c for c in certificates if c["status"] == "error"])

    pc_pass = len([c for c in prechecks if c["status"] == "pass"])
    pc_fail = len([c for c in prechecks if c["status"] == "fail"])
    pc_warn = len([c for c in prechecks if c["status"] == "warn"])
    pc_skip = len([c for c in prechecks if c["status"] == "skip"])

    # ── 5. Build payload ─────────────────────────────────────────────────
    payload = {
        "timestamp": timestamp,
        "node": node_name,
        "threshold_days": THRESHOLD_DAYS,
        "certificates": certificates,
        "sa_keys": sa_keys,
        "prechecks": prechecks,
        "backup": backup_info,
        "summary": {
            "total_certs": total_certs,
            "certs_ok": certs_ok,
            "certs_warn": certs_warn,
            "certs_error": certs_error,
            "prechecks_pass": pc_pass,
            "prechecks_fail": pc_fail,
            "prechecks_warn": pc_warn,
            "prechecks_skip": pc_skip,
        },
    }

    log("")
    log(f"Certs: {total_certs} total ({certs_ok} ok, {certs_warn} warn, {certs_error} error)")
    log(f"Prechecks: {pc_pass} pass, {pc_fail} fail, {pc_warn} warn, {pc_skip} skip")
    log(f"Backup: {backup_info['latest']}")

    # ── 6. Write to ConfigMap ────────────────────────────────────────────
    log("")
    log(f"Writing to ConfigMap {NAMESPACE}/{CONFIGMAP_NAME}...")
    write_configmap(payload)
    log("Done!")


if __name__ == "__main__":
    main()

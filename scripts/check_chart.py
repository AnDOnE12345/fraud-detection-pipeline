"""Render real Helm modes and check deployment invariants (no cluster needed)."""
import os
import subprocess
from pathlib import Path
import yaml

HELM = os.getenv("HELM_BIN", "helm")
CHART = str(Path(__file__).resolve().parents[1] / "deploy/helm/fraud-pipeline")


def render(*args):
    result = subprocess.run([HELM, "template", "fraud-pipeline", CHART, *args],
                            capture_output=True, text=True, check=True)
    docs = [doc for doc in yaml.safe_load_all(result.stdout) if doc]
    return {(doc["kind"], doc["metadata"]["name"]): doc for doc in docs}


def check():
    for scale in (False, True):
        for keda in (False, True):
            args = ["--set", f"keda.enabled={str(keda).lower()}"]
            if scale:
                args += ["-f", str(Path(CHART) / "values-scale.yaml")]
            docs = render(*args)
            assert docs["StatefulSet", "kafka"]["spec"]["replicas"] == (3 if scale else 1)
            assert docs["StatefulSet", "minio"]["spec"]["replicas"] == (4 if scale else 1)
            for app in ("kafka", "minio"):
                assert docs["Service", f"{app}-headless"]["spec"]["clusterIP"] == "None"
                assert docs["StatefulSet", app]["spec"]["volumeClaimTemplates"]
            for app in ("producer", "serving", "ui"):
                assert "replicas" not in docs["Deployment", app]["spec"]
                assert docs["HorizontalPodAutoscaler", f"{app}-hpa"]["spec"]["minReplicas"] == (2 if scale else 1)
            assert ("replicas" not in docs["Deployment", "processor"]["spec"]) == keda
            for (kind, _), doc in docs.items():
                if kind in ("Deployment", "StatefulSet"):
                    for container in doc["spec"]["template"]["spec"]["containers"]:
                        assert container["resources"]["requests"]
                        assert container["readinessProbe"] and container["livenessProbe"]
            print(f"PASS scale={scale}, keda={keda}: {len(docs)} resources")
    expanded = render("-f", str(Path(CHART) / "values-scale.yaml"), "--set", "minio.poolCount=2")
    minio = expanded["StatefulSet", "minio"]["spec"]
    assert minio["replicas"] == 8
    args = minio["template"]["spec"]["containers"][0]["args"]
    assert "http://minio-{4...7}.minio-headless:9000/data" in args
    invalid = subprocess.run([HELM, "template", "test", CHART, "--set", "kafka.replicationFactor=3"],
                             capture_output=True, text=True)
    assert invalid.returncode != 0
    print("PASS pool expansion and invalid replication factor rejection")


if __name__ == "__main__":
    check()

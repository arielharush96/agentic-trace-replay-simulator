import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from run_experiment import rewrite_appworld_claim_name, verify_appworld_data


def test_rewrite_appworld_claim_name_binds_the_configured_pvc():
    manifest = "volumes:\n- name: appworld-data\n  persistentVolumeClaim:\n    claimName: trace-replay-appworld-data\n"

    rewritten = rewrite_appworld_claim_name(manifest, "customer-appworld-data")

    assert "claimName: customer-appworld-data" in rewritten
    assert "trace-replay-appworld-data" not in rewritten


def test_rewrite_appworld_claim_name_rejects_invalid_names():
    with pytest.raises(RuntimeError, match="invalid AppWorld PVC name"):
        rewrite_appworld_claim_name("claimName: old\n", "Invalid_Name")


class _MissingPvcCluster:
    def __init__(self):
        self.run_calls = []

    def run(self, args, **_kwargs):
        self.run_calls.append(args)
        return ""

    def delete_owned(self, *_args, **_kwargs):
        raise AssertionError("nothing should be deleted for a missing PVC")


def test_verify_appworld_data_requires_the_configured_pvc():
    cluster = _MissingPvcCluster()

    with pytest.raises(RuntimeError, match="customer-appworld-data.*missing"):
        verify_appworld_data(cluster, "trace-replay", "node-a", "customer-appworld-data")

    assert cluster.run_calls == [["-n", "trace-replay", "get", "pvc", "customer-appworld-data"]]

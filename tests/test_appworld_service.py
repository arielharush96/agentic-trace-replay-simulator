import json
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))

from appworld_service import _render_call


def test_render_call_binds_boolean_and_null_arguments():
    calls = []
    apis = SimpleNamespace(
        gmail=SimpleNamespace(send_email=lambda **kwargs: calls.append(kwargs) or {"ok": True}),
    )
    code = _render_call(
        "response = apis.gmail.send_email(**arguments)",
        {"draft": True, "body": None, "note": "it's 100%"},
    )

    exec("import json\n" + code, {"apis": apis, "json": json})

    assert calls == [{"draft": True, "body": None, "note": "it's 100%"}]

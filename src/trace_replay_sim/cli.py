"""CLI for the OpenClaw/OpenShell trace-replay simulator."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from trace_replay_sim.analyze import write_report
from trace_replay_sim.collect import collect
from trace_replay_sim.driver import drive
from trace_replay_sim.ingest import ingest
from trace_replay_sim.mock_llm import DELAY_PROFILES, is_phantom_turn, serve


def build_corpus(
    *, in_path: Path, out_path: Path, session_index: int | None,
    session_id: str | None, strip_phantom: bool,
) -> dict:
    """Select one session from a corpus file and (optionally) drop phantom turns."""
    payload = json.loads(in_path.read_text(encoding="utf-8"))
    sessions = payload.get("sessions") or []
    if session_id:
        chosen = next((s for s in sessions if str(s.get("session_id")) == session_id), None)
    elif session_index is not None:
        chosen = sessions[session_index] if 0 <= session_index < len(sessions) else None
    else:
        chosen = sessions[0] if sessions else None
    if chosen is None:
        raise SystemExit(f"no session matched (index={session_index}, id={session_id})")

    turns = list(chosen.get("turns") or [])
    dropped = 0
    if strip_phantom:
        kept = [t for t in turns if not is_phantom_turn(t)]
        dropped = len(turns) - len(kept)
        for i, t in enumerate(kept):
            t["index"] = i
        chosen = dict(chosen, turns=kept)

    out = {
        "dataset": payload.get("dataset", "replay"),
        "session_count": 1,
        "source": str(in_path),
        "selected_session_id": chosen.get("session_id"),
        "real_turns": len(chosen.get("turns") or []),
        "phantom_dropped": dropped,
        "sessions": [chosen],
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    return {
        "selected_session_id": chosen.get("session_id"),
        "real_turns": out["real_turns"],
        "phantom_dropped": dropped,
        "out": str(out_path),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="trace-replay-sim")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_ing = sub.add_parser("ingest", help="Download/parse Exgentic traces into a replay corpus")
    p_ing.add_argument("--out", required=True)
    p_ing.add_argument("--dataset", default="Exgentic/agent-llm-traces")
    p_ing.add_argument("--local")
    p_ing.add_argument("--limit", type=int, default=50)
    p_ing.add_argument("--harness")
    p_ing.add_argument("--benchmark")

    p_cor = sub.add_parser("corpus", help="Select one session from a corpus and strip phantom turns")
    p_cor.add_argument("--in", dest="in_path", required=True)
    p_cor.add_argument("--out", required=True)
    p_cor.add_argument("--session-index", type=int, default=None)
    p_cor.add_argument("--session-id", default=None)
    p_cor.add_argument("--keep-phantom", action="store_true", help="do not strip phantom turns")

    p_mock = sub.add_parser("mock-llm", help="Run the local mock LLM server")
    p_mock.add_argument("--corpus", required=True)
    p_mock.add_argument("--host", default="0.0.0.0")
    p_mock.add_argument("--port", type=int, default=8080)
    p_mock.add_argument("--mode", choices=["text", "turns"], default="turns")
    p_mock.add_argument("--timing", default="rhaiis",
                        help="'instant' or a profile: " + ", ".join(sorted(DELAY_PROFILES)) + ", env")
    p_mock.add_argument("--edges", default=None, help="path to write per-turn edge JSONL")

    p_drv = sub.add_parser("drive", help="Replay user turns against mock-LLM or OpenClaw")
    p_drv.add_argument("--corpus", required=True)
    p_drv.add_argument("--out", required=True)
    p_drv.add_argument("--layer", choices=["mock-direct", "openclaw", "shell"], required=True)
    p_drv.add_argument("--url", required=True)
    p_drv.add_argument("--model", default="openclaw/perf_agent")
    p_drv.add_argument("--token", default="")
    p_drv.add_argument("--concurrency", type=int, default=1)
    p_drv.add_argument("--limit", type=int, default=None)
    p_drv.add_argument("--timeout", type=float, default=120.0)
    p_drv.add_argument("--stream", action="store_true")
    p_drv.add_argument("--no-spans", action="store_true", help="disable OTLP span export")
    p_drv.add_argument("--session-turns", action="store_true",
                       help="send one request per corpus turn while reusing one OpenClaw session")
    p_drv.add_argument("--prewarm", action="store_true",
                       help="prewarm the session sandbox before measured turns")

    p_col = sub.add_parser("collect", help="Collect Prometheus CPU/memory for a time window")
    p_col.add_argument("--out", required=True)
    p_col.add_argument("--start", type=float, required=True)
    p_col.add_argument("--end", type=float, required=True)
    p_col.add_argument("--ns-openclaw", default="trace-replay")
    p_col.add_argument("--ns-openshell", default="openshell-tracesim")
    p_col.add_argument("--openclaw-pod", default=None)
    p_col.add_argument("--thanos-host", default=os.environ.get("THANOS_HOST"))
    p_col.add_argument("--openclaw-url", default=os.environ.get("OPENCLAW_URL"))
    p_col.add_argument("--openclaw-api-key", default=os.environ.get("OPENCLAW_TOKEN"))

    p_der = sub.add_parser("derive", help="Fuse mock/Jaeger/K8s sources into per-turn 5-bucket records")
    p_der.add_argument("--out", required=True)
    p_der.add_argument("--mock-edges", required=True)
    p_der.add_argument("--traces", default=None, help="raw_traces.json from Jaeger export")
    p_der.add_argument("--driver-requests", default=None)
    p_der.add_argument("--k8s", default=None, help="sandbox pod lifecycle JSON (cold Ready-delta)")

    p_dia = sub.add_parser("diagram", help="Render the per-turn 5-bucket session waterfall")
    p_dia.add_argument("--per-turn", required=True, help="per_turn.json from `derive`")
    p_dia.add_argument("--results", default=None, help="results dir with layer subdirs")
    p_dia.add_argument("--out", required=True)
    p_dia.add_argument("--exclude", default=None,
                       help="comma-separated buckets to omit from the waterfall (letters A-E or keys)")

    p_an = sub.add_parser("analyze", help="Write REPORT.md from one or more layer run dirs")
    p_an.add_argument("--results-dir", required=True)

    p_prof = sub.add_parser("profile", help="Profile multi-turn harness phase boundaries")
    p_prof.add_argument("--per-turn", required=True, help="per_turn.json from derive")
    p_prof.add_argument("--out", required=True)
    p_prof.add_argument("--driver-requests", default=None)
    p_prof.add_argument("--warm-only", action="store_true")

    p_v2 = sub.add_parser("profile-v2", help="Build an evidence-first OpenClaw OTEL multi-turn profile")
    p_v2.add_argument("--traces", required=True)
    p_v2.add_argument("--mock-edges", default=None)
    p_v2.add_argument("--prom", default=None)
    p_v2.add_argument("--driver-requests", default=None)
    p_v2.add_argument("--out", required=True)

    p_audit = sub.add_parser("audit-corpus", help="Audit replay fidelity and classify sessions")
    p_audit.add_argument("--corpus", required=True)
    p_audit.add_argument("--out", required=True)

    p_study = sub.add_parser("study-v3", help="Generate persisted multi-cycle research analysis")
    p_study.add_argument("--root", required=True)
    p_study.add_argument("--out", required=True)

    args = parser.parse_args(argv)
    if args.cmd == "ingest":
        extra = ingest(
            out_path=Path(args.out),
            dataset=args.dataset,
            local_path=Path(args.local) if args.local else None,
            limit=args.limit,
            harness=args.harness,
            benchmark=args.benchmark,
        )
        print(json.dumps(extra, indent=2))
        return 0
    if args.cmd == "corpus":
        info = build_corpus(
            in_path=Path(args.in_path),
            out_path=Path(args.out),
            session_index=args.session_index,
            session_id=args.session_id,
            strip_phantom=not args.keep_phantom,
        )
        print(json.dumps(info, indent=2))
        return 0
    if args.cmd == "mock-llm":
        serve(args.host, args.port, args.corpus, mode=args.mode, timing=args.timing, edges_path=args.edges)
        return 0
    if args.cmd == "drive":
        if args.session_turns:
            from trace_replay_sim.driver import drive_session_turns
            summary = drive_session_turns(
                corpus=Path(args.corpus), out_dir=Path(args.out), layer=args.layer,
                url=args.url, model=args.model, token=args.token,
                timeout=args.timeout, stream=args.stream,
                export_spans=not args.no_spans,
                prewarm=args.prewarm,
            )
        else:
            summary = drive(
                corpus=Path(args.corpus), out_dir=Path(args.out), layer=args.layer,
                url=args.url, model=args.model, token=args.token,
                concurrency=args.concurrency, limit=args.limit,
                timeout=args.timeout, stream=args.stream,
                export_spans=not args.no_spans,
            )
        print(json.dumps(summary, indent=2))
        return 0 if summary["errors"] == 0 else 2
    if args.cmd == "collect":
        report = collect(
            Path(args.out),
            start=args.start,
            end=args.end,
            ns_openclaw=args.ns_openclaw,
            ns_openshell=args.ns_openshell,
            openclaw_pod=args.openclaw_pod,
            thanos_host=args.thanos_host,
            openclaw_url=args.openclaw_url,
            openclaw_api_key=args.openclaw_api_key,
        )
        print(json.dumps(report, indent=2))
        return 0
    if args.cmd == "derive":
        from trace_replay_sim.derive import derive as run_derive
        summary = run_derive(
            out_dir=Path(args.out),
            mock_edges=Path(args.mock_edges),
            traces=Path(args.traces) if args.traces else None,
            driver_requests=Path(args.driver_requests) if args.driver_requests else None,
            k8s=Path(args.k8s) if args.k8s else None,
        )
        print(json.dumps(summary, indent=2))
        return 0
    if args.cmd == "diagram":
        from trace_replay_sim.timing_diagram import generate_all
        exclude = set(args.exclude.split(",")) if args.exclude else None
        generate_all(
            Path(args.per_turn),
            Path(args.results) if args.results else None,
            Path(args.out),
            exclude=exclude,
        )
        return 0
    if args.cmd == "profile":
        from trace_replay_sim.multi_turn_profile import profile as run_profile
        summary = run_profile(
            per_turn=Path(args.per_turn),
            out=Path(args.out),
            driver_requests=Path(args.driver_requests) if args.driver_requests else None,
            warm_only=args.warm_only,
        )
        print(json.dumps(summary, indent=2))
        return 0
    if args.cmd == "profile-v2":
        from trace_replay_sim.profiler_v2 import profile
        summary = profile(
            traces=Path(args.traces),
            mock_edges=Path(args.mock_edges) if args.mock_edges else None,
            prom=Path(args.prom) if args.prom else None,
            driver_requests=Path(args.driver_requests) if args.driver_requests else None,
            out=Path(args.out),
        )
        print(json.dumps(summary, indent=2))
        return 0
    if args.cmd == "audit-corpus":
        from trace_replay_sim.audit_corpus import audit_corpus
        summary = audit_corpus(Path(args.corpus), Path(args.out))
        print(json.dumps({key: value for key, value in summary.items() if key != "sessions_detail"}, indent=2))
        return 0
    if args.cmd == "study-v3":
        from trace_replay_sim.study_v3 import study
        summary = study(Path(args.root), Path(args.out))
        print(json.dumps(summary, indent=2))
        return 0
    report = write_report(Path(args.results_dir))
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

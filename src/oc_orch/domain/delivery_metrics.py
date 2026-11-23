"""固定cohort/窗口的交付度量纯函数；缺失保分母，force/混合轨迹不冒领收益。"""
import math
import statistics

from .delivery_policy import identity, fingerprint
from .errors import LedgerError

COMPONENTS = ("coordinator", "worker", "review", "retry", "compaction")


def invalid(message):
    raise LedgerError("DELIVERY_METRICS_INVALID", message)


def stamp(value):
    return type(value) is int and value >= 0


def ratio(numerator, denominator):
    return {"numerator": numerator, "denominator": denominator,
            "value": numerator / denominator if denominator else None}


def distribution(values):
    values = sorted(values)
    return {"n": len(values), "median_ms": statistics.median(values) if values else None,
            "p90_ms": values[math.ceil(len(values) * .9) - 1] if values else None,
            "p90_method": "nearest-rank"}


def report(cohort, observations, *, as_of=None):
    """输入是已固封的事实导出，不从成功样本倒推cohort；本函数不授予验收。"""
    if (not isinstance(cohort, dict) or set(cohort) != {"schema", "fixed_at", "window", "conditions", "members"}
            or cohort["schema"] != "oc-orch.delivery-cohort.v1" or not stamp(cohort["fixed_at"])
            or not isinstance(cohort["conditions"], dict) or not isinstance(cohort["members"], list)
            or not isinstance(cohort["window"], dict) or set(cohort["window"]) != {"start", "end"}):
        invalid("须事前固定cohort、窗口与资源条件")
    start, end = cohort["window"]["start"], cohort["window"]["end"]
    if not stamp(start) or not stamp(end) or not cohort["fixed_at"] <= start < end:
        invalid("窗口/固定时点非法")
    if as_of is not None:
        if not stamp(as_of) or not start <= as_of <= end: invalid("观测截止必须在预定窗口内")
        end = as_of
    members = {}
    for member in cohort["members"]:
        if not isinstance(member, dict) or set(member) != {"task_id", "arm", "scope"}:
            invalid("cohort成员只接受task_id/arm/scope")
        for key in ("task_id", "arm"): identity(member[key], key)
        if member["scope"] not in ("standard", "prototype") or member["task_id"] in members:
            invalid("cohort身份重复或范围未知")
        members[member["task_id"]] = member
    if not isinstance(observations, list): invalid("observations须为列表")
    facts = {}
    for fact in observations:
        if not isinstance(fact, dict) or fact.get("task_id") not in members or fact["task_id"] in facts:
            invalid("拒绝cohort外/重复任务，不能临时拼接成功样本")
        identity(fact.get("source"), "source")
        if set(fact) - {"task_id", "source", "dispatch_intent_at", "first_action_at", "result_ready_at", "review_started_at",
                        "outcome", "verifications", "permits", "switches", "refusals", "usage"}:
            invalid("未知事实字段")
        facts[fact["task_id"]] = fact
    rows, permits, switch_delays, refusals = [], [], [], 0
    usage = {component: {"known_tokens": 0, "known_amount": 0, "amount_by_currency": {}, "unknown_tasks": 0, "currencies": set(), "bases": set()}
             for component in COMPONENTS}
    for tid, member in members.items():
        fact = facts.get(tid, {})
        intent = fact.get("dispatch_intent_at")
        if intent is not None and (not stamp(intent) or not start <= intent <= end):
            invalid("首次派单意图不在预定窗口内；不得静默删样本")
        events = fact.get("verifications", [])
        if not isinstance(events, list): invalid("verifications须为列表")
        for event in events:
            if (not isinstance(event, dict) or set(event) != {"at", "result", "classification"}
                    or not stamp(event["at"]) or event["at"] < start or (intent is not None and event["at"] < intent)
                    or event["result"] not in ("pass", "fail")
                    or event["classification"] not in ("normal_effective", "forced", "fail")
                    or (event["result"] == "fail") != (event["classification"] == "fail")):
                invalid("正式验收事实非法")
        in_window = sorted((e for e in events if e["at"] <= end), key=lambda e: e["at"])
        accepted = in_window[-1] if in_window and in_window[-1]["classification"] == "normal_effective" else None
        own_permits = fact.get("permits", [])
        if not isinstance(own_permits, list): invalid("permits须为列表")
        for permit in own_permits:
            if (not isinstance(permit, dict) or set(permit) != {"at", "revision", "effective_revision", "authorized"}
                    or not stamp(permit["at"]) or type(permit["authorized"]) is not bool
                    or any(type(permit[k]) is not int or permit[k] < 1 for k in ("revision", "effective_revision"))):
                invalid("许可必须含当时有效版本与独立授权结果")
        own_permits = [p for p in own_permits if start <= p["at"] <= end]
        permits.extend(own_permits)
        switches = fact.get("switches", [])
        if not isinstance(switches, list): invalid("switches须为列表")
        for switch in switches:
            if (not isinstance(switch, dict) or set(switch) != {"at", "revision"} or not stamp(switch["at"])
                    or type(switch["revision"]) is not int or switch["revision"] < 1):
                invalid("切换事件非法")
            if not start <= switch["at"] <= end: continue
            following = [p["at"] - switch["at"] for p in own_permits
                         if p["at"] >= switch["at"] and p["revision"] == switch["revision"]
                         and p["effective_revision"] == switch["revision"] and p["authorized"]]
            switch_delays.append(min(following) if following else None)
        count = fact.get("refusals", 0)
        if type(count) is not int or count < 0: invalid("拒绝数须独立非负整数")
        refusals += count
        def interval(left, right):
            a, b = fact.get(left), fact.get(right)
            if a is None or b is None: return None
            if not stamp(a) or not stamp(b) or a > b: invalid("时点倒退/形状非法")
            return b - a if b <= end else None
        entries = fact.get("usage", [])
        if not isinstance(entries, list): invalid("usage须为列表")
        incomplete_usage = set()
        for component in COMPONENTS:
            entries_for_component = [u for u in entries if isinstance(u, dict) and u.get("component") == component]
            if not entries_for_component: incomplete_usage.add(component)
        for entry in entries:
            if (not isinstance(entry, dict) or set(entry) != {"component", "tokens", "amount", "currency", "basis", "coverage", "source"}
                    or entry["component"] not in COMPONENTS or entry["coverage"] not in ("input-only", "full-chain")
                    or entry["basis"] not in ("user-zero", "provider-report", "billing")):
                invalid("费用/用量必须分组件、来源、覆盖与记账口径")
            identity(entry["source"], "usage.source"); identity(entry["currency"], "currency")
            tokens, amount = entry["tokens"], entry["amount"]
            if tokens is not None and (type(tokens) is not int or tokens < 0): invalid("tokens须非负整数或unknown")
            if amount is not None and (type(amount) not in (int, float) or not math.isfinite(amount) or amount < 0): invalid("金额须非负有限数或unknown")
            group = usage[entry["component"]]
            group["known_tokens"] += tokens or 0
            group["known_amount"] += amount or 0
            group["amount_by_currency"][entry["currency"]] = group["amount_by_currency"].get(entry["currency"], 0) + (amount or 0)
            group["currencies"].add(entry["currency"]); group["bases"].add(entry["basis"])
            if tokens is None or amount is None or entry["coverage"] != "full-chain": incomplete_usage.add(entry["component"])
        for component in incomplete_usage: usage[component]["unknown_tasks"] += 1
        rows.append({**member, "accepted": accepted is not None, "latency_ms": accepted["at"] - intent if accepted and intent is not None else None,
                     "first_action_ms": interval("dispatch_intent_at", "first_action_at"),
                     "review_wait_ms": interval("result_ready_at", "review_started_at"),
                     "first_verification_pass": bool(in_window and in_window[0]["classification"] == "normal_effective"),
                     "entered_verification": bool(in_window), "unresolved_age_ms": end - intent if not accepted and intent is not None else None,
                     "outcome": fact.get("outcome", "unknown"), "mixed_policy": len({p["revision"] for p in own_permits}) > 1 or any(start <= s["at"] <= end for s in switches),
                     "missing_observation": tid not in facts})
    accepted = sum(r["accepted"] for r in rows)
    grouped = {}
    for row in rows:
        key = f"{row['scope']}:{row['arm']}:{'mixed' if row['mixed_policy'] else 'stable'}"
        grouped.setdefault(key, []).append(row)
    def summary(items):
        n = sum(r["accepted"] for r in items)
        return {"delivery_rate": ratio(n, len(items)), "latency": distribution([r["latency_ms"] for r in items if r["latency_ms"] is not None]),
                "unresolved_rate": ratio(len(items) - n, len(items)),
                "unresolved_ages_ms": [r["unresolved_age_ms"] for r in items if not r["accepted"]]}
    for component, values in usage.items():
        values["currencies"] = sorted(values["currencies"]); values["bases"] = sorted(values["bases"])
        values["amount_per_effective_delivery"] = (values["known_amount"] / accepted
            if accepted and not values["unknown_tasks"] and len(values["currencies"]) == 1 else None)
        if len(values["currencies"]) > 1: values["known_amount"] = None
    return {"ok": True, "schema": "oc-orch.delivery-metrics.v1", "cohort_sha256": fingerprint(cohort),
            "window": {"start": start, "end": end}, "planned_window": cohort["window"], "interim": end < cohort["window"]["end"],
            "summary": summary(rows), "groups": {k: summary(v) for k, v in grouped.items()},
            "first_pass_rate": ratio(sum(r["first_verification_pass"] for r in rows), sum(r["entered_verification"] for r in rows)),
            "not_entered_verification": sum(not r["entered_verification"] for r in rows),
            "first_action": distribution([r["first_action_ms"] for r in rows if r["first_action_ms"] is not None]),
            "review_wait": distribution([r["review_wait_ms"] for r in rows if r["review_wait_ms"] is not None]),
            "switch_consistency": ratio(sum(p["authorized"] and p["revision"] == p["effective_revision"] for p in permits), len(permits)),
            "unauthorized_or_stale_permits": sum(not p["authorized"] or p["revision"] != p["effective_revision"] for p in permits),
            "refusals": refusals, "switch_propagation": {**distribution([v for v in switch_delays if v is not None]),
                                                         "not_observed": switch_delays.count(None)},
            "usage": usage, "tasks": rows, "speedup_proven": False,
            "limits": ["输入须来自固定cohort的实际事实导出；计算不授予业务验收", "失败/未结/取消/unknown保分母；force不计正常有效交付",
                       "混合策略单列，不将收益归给最终预设；分母0为null", "已知用量小计不是全链费用；本报告不推断因果提速或生产SLA"]}

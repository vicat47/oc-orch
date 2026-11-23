"""快速原型范围及缺口值；不增加Task状态机或另一套正式验收。"""
import json

from .delivery_policy import identity, canonical
from .errors import LedgerError

STATES = ("real", "mock", "placeholder", "blocked")
GAP_STATES = ("open", "deferred", "risk-accepted", "fixed")


def reject(message):
    raise LedgerError("PROTOTYPE_INVALID", message)


def strings(values, label, maximum=64, nonempty=True):
    if not isinstance(values, list) or len(values) > maximum or (nonempty and not values):
        reject(label + "须为有界非空列表")
    for value in values:
        identity(value, label)
    if len(set(values)) != len(values):
        reject(label + "不能重复")


def normalize(plan):
    if not isinstance(plan, dict) or set(plan) != {"question", "entry", "startup", "checks", "paths", "features", "gaps"}:
        reject("原型计划仅接受question/entry/startup/checks/paths/features/gaps")
    for key in ("question", "entry", "startup"):
        identity(plan[key], key)
    strings(plan["checks"], "checks", 32)
    features, gaps, paths = plan["features"], plan["gaps"], plan["paths"]
    if not isinstance(features, list) or not 1 <= len(features) <= 32:
        reject("features须为1..32项")
    if not isinstance(gaps, list) or len(gaps) > 64:
        reject("gaps最多64项")
    if not isinstance(paths, list) or not 1 <= len(paths) <= 16:
        reject("paths须为1..16项")
    feature_ids, gap_ids, path_ids = set(), set(), set()
    for gap in gaps:
        fields = {"id", "kind", "state", "risk", "expected", "current", "reason", "impact", "owner", "next_action", "review_at", "refs", "followup_task", "evidence"}
        if not isinstance(gap, dict) or set(gap) != fields:
            reject("缺口字段不完整或未知")
        for key in ("id", "expected", "current", "reason", "impact", "owner", "next_action", "review_at"):
            identity(gap[key], key)
        if gap["id"] in gap_ids:
            reject("缺口身份重复")
        gap_ids.add(gap["id"])
        if gap["kind"] not in ("tradeoff", "defect", "pitfall") or gap["state"] not in GAP_STATES or gap["risk"] not in ("ordinary", "security", "integrity", "permission"):
            reject("缺口分类/状态/风险非法")
        if gap["risk"] != "ordinary" and gap["state"] in ("deferred", "risk-accepted"):
            reject("安全/数据完整性/权限缺口不得靠原型延期或接受风险豁免")
        strings(gap["refs"], "refs")
        if gap["followup_task"] is not None:
            identity(gap["followup_task"], "followup_task")
        if gap["evidence"] is not None:
            proof = gap["evidence"]
            if not isinstance(proof, dict) or set(proof) != {"attempt_id", "verified_at"} or type(proof["verified_at"]) is not int or proof["verified_at"] <= 0:
                reject("修复须引用正式验收attempt_id/verified_at")
            identity(proof["attempt_id"], "attempt_id")
        if gap["state"] == "fixed" and gap["evidence"] is None:
            reject("标记已修复须有真实验收引用，记账不等于修复")
    for feature in features:
        if not isinstance(feature, dict) or set(feature) != {"id", "title", "state", "source", "gap_ids"}:
            reject("功能字段不完整或未知")
        for key in ("id", "title", "source"):
            identity(feature[key], key)
        if feature["id"] in feature_ids or feature["state"] not in STATES:
            reject("功能身份重复或状态非法")
        feature_ids.add(feature["id"])
        strings(feature["gap_ids"], "gap_ids", nonempty=feature["state"] != "real")
        if not set(feature["gap_ids"]) <= gap_ids:
            reject("功能必须引用权威缺口，不能无责任/下一动作")
    for path in paths:
        if not isinstance(path, dict) or set(path) != {"id", "features"}:
            reject("代表路径字段非法")
        identity(path["id"], "path_id")
        strings(path["features"], "path_features", 32)
        if path["id"] in path_ids or not set(path["features"]) <= feature_ids:
            reject("代表路径身份/功能引用不一致")
        path_ids.add(path["id"])
    if len(canonical(plan).encode()) > 200000:
        reject("计划超过200KB")
    return json.loads(canonical(plan))


def preserve(previous, current):
    old_gaps, new_gaps = {g["id"]: g for g in previous["gaps"]}, {g["id"]: g for g in current["gaps"]}
    old_features, new_features = {f["id"]: f for f in previous["features"]}, {f["id"]: f for f in current["features"]}
    if (not old_gaps.keys() <= new_gaps.keys() or not old_features.keys() <= new_features.keys()
            or not {p["id"] for p in previous["paths"]} <= {p["id"] for p in current["paths"]}):
        reject("原缺口/功能身份不得删除；完成、暂缓与风险接受分开保留")
    for key, gap in old_gaps.items():
        if any(gap[name] != new_gaps[key][name] for name in ("kind", "risk", "expected")):
            reject("原缺陷不得改名取舍或改低原期望；新范围另增记录")
    for key, feature in old_features.items():
        if feature["state"] != "real" and new_features[key]["state"] == "real":
            if not set(feature["gap_ids"]) <= set(new_features[key]["gap_ids"]):
                reject("真实接入仍须保留原缺口引用")
            if any(new_gaps[g]["state"] != "fixed" for g in feature["gap_ids"]):
                reject("真实接入须由原缺口修复证据支持")

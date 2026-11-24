"""结果流恢复规划逻辑。

规划器是纯函数：只根据调用方传入的快照状态、确认范围、排序键、
分页边界、事务有效期、副本位置和 resume token 决定下一次读取位置，
不连接数据库、不执行 SQL、也不触碰真实行数据。
"""

from .exceptions import InvalidResumeTokenError
from .models import (
    Checkpoint,
    DuplicateRisk,
    NextCursor,
    RecoveryMethod,
    RecoveryPlan,
    RecoveryRequest,
    RiskLevel,
    RowRange,
    SortDirection,
)


def plan_recovery(request):
    """根据重连上下文计算恢复方案。

    返回 :class:`RecoveryPlan`；当 resume token 与游标不属于同一次
    读取（cursor 不匹配）或排序方向不受支持时抛出
    :class:`InvalidResumeTokenError`。
    """
    if not isinstance(request, RecoveryRequest):
        raise TypeError("request 必须是 RecoveryRequest 实例")

    token = request.resume_token
    _validate_token_context(request, token)

    merged = _merge_ranges(request.acknowledged_ranges)
    checkpoint = _contiguous_checkpoint(merged)
    token_replayed = token.token_id in request.consumed_token_ids

    snapshot_errors = _snapshot_errors(request, token)
    if snapshot_errors:
        return _requery_plan(request, checkpoint, merged, snapshot_errors)

    if not request.sort_key.unique:
        return _tie_group_plan(request, checkpoint, merged, token_replayed)

    return _unique_key_plan(request, checkpoint, merged, token, token_replayed)


def _validate_token_context(request, token):
    if token.cursor_id != request.cursor_id:
        raise InvalidResumeTokenError(
            "resume token 属于游标 %r，与当前游标 %r 不匹配"
            % (token.cursor_id, request.cursor_id)
        )
    if request.sort_key.direction != SortDirection.ASC:
        raise InvalidResumeTokenError(
            "规划器目前只支持 ASC 排序键，收到 %r" % request.sort_key.direction
        )
    if token.boundary_key > request.pagination_boundary.last_key:
        raise InvalidResumeTokenError(
            "resume token 边界 %r 超出服务端分页边界 %r"
            % (token.boundary_key, request.pagination_boundary.last_key)
        )


def _snapshot_errors(request, token):
    """返回快照失效原因列表；为空表示快照仍可用于续读。"""
    errors = []
    snapshot = request.snapshot

    if snapshot.state.value == "EXPIRED":
        errors.append("查询快照已过期")
    elif snapshot.state.value == "UNAVAILABLE":
        errors.append("查询快照在服务端已不可用")

    if not request.transaction.alive:
        if request.transaction.deadline_utc is None:
            errors.append("事务已终止，快照无法继续使用")
        else:
            errors.append(
                "事务已超过有效期（deadline=%s, now=%s）"
                % (request.transaction.deadline_utc, request.transaction.now_utc)
            )

    if not request.replica.retains_snapshot:
        errors.append(
            "副本 %s 已回放到版本 %s，晚于快照所需版本 %s，旧版本被清理"
            % (
                request.replica.replica_id,
                request.replica.current_version,
                request.replica.required_version,
            )
        )

    if token.snapshot_id != snapshot.snapshot_id or token.generation != snapshot.generation:
        errors.append(
            "resume token 指向旧快照 %s#g%s，当前快照为 %s#g%s，跨快照不能拼接"
            % (
                token.snapshot_id,
                token.generation,
                snapshot.snapshot_id,
                snapshot.generation,
            )
        )
    return errors


def _requery_plan(request, checkpoint, merged, errors):
    """快照失效：必须重新查询，旧流整体丢弃，禁止拼接。"""
    reason = "；".join(errors)
    discarded = tuple(
        RowRange(
            start_key=row_range.start_key,
            end_key=row_range.end_key,
            row_count=row_range.row_count,
        )
        for row_range in merged
    )
    next_cursor = NextCursor(
        cursor_id=request.cursor_id,
        use_existing_cursor=False,
        after_key=None,
        snapshot_id=request.snapshot.snapshot_id,
        note="需要基于全新快照打开新游标，旧游标作废",
    )
    return RecoveryPlan(
        method=RecoveryMethod.REQUERY,
        next_cursor=next_cursor,
        duplicate_risk=DuplicateRisk(
            level=RiskLevel.FULL_STREAM,
            detail="重新查询覆盖整个结果集，已交付行只能由调用方按业务键去重",
        ),
        discarded_ranges=discarded,
        checkpoint=checkpoint,
        safe_to_splice=False,
        reason=reason,
        warnings=("禁止将旧快照结果与新快照结果直接拼接",),
    )


def _tie_group_plan(request, checkpoint, merged, token_replayed):
    """无唯一排序键：无法精确恢复，只能重读边界等值分组。"""
    token = request.resume_token
    tie_value = request.sort_key.tie_group_value
    if tie_value is None:
        tie_value = token.boundary_key

    warnings = [
        "排序键 %r 不唯一，规划器不能保证精确恢复，边界等值分组必须重读并由调用方去重"
        % list(request.sort_key.columns)
    ]
    if token_replayed:
        warnings.append(
            "resume token %s 已被消费过，重读分组可能再次返回已交付行" % token.token_id
        )

    discarded = (
        RowRange(start_key=tie_value, end_key=tie_value, row_count=0),
    )
    next_cursor = NextCursor(
        cursor_id=request.cursor_id,
        use_existing_cursor=True,
        after_key=tie_value,
        snapshot_id=request.snapshot.snapshot_id,
        note="重读排序键等于 %r 的整个等值分组，after_key 仅作定位需配合去重" % (tie_value,),
    )
    return RecoveryPlan(
        method=RecoveryMethod.REREAD_TIE_GROUP,
        next_cursor=next_cursor,
        duplicate_risk=DuplicateRisk(
            level=RiskLevel.TIE_GROUP,
            detail="等值分组内行顺序不稳定，组内已交付行可能重复、未交付行不得遗漏，调用方必须按业务键去重",
        ),
        discarded_ranges=discarded,
        checkpoint=checkpoint,
        safe_to_splice=False,
        reason="排序键非唯一，无法在等值分组内部定位精确断点",
        warnings=tuple(warnings),
    )


def _unique_key_plan(request, checkpoint, merged, token, token_replayed):
    """唯一排序键 + 快照有效：可以做到不遗漏、不重复。"""
    covered = _range_covering(merged, token.boundary_key, token.boundary_key)

    if covered:
        advance_to = checkpoint.key if checkpoint.key is not None else token.boundary_key
        skipped = RowRange(
            start_key=token.boundary_key,
            end_key=advance_to,
            row_count=checkpoint.row_count,
        )
        warnings = []
        if token_replayed:
            warnings.append(
                "重复提交 token %s，但其边界已全部确认，直接推进到确认高水位" % token.token_id
            )
        return RecoveryPlan(
            method=RecoveryMethod.RESUME_ADVANCED,
            next_cursor=NextCursor(
                cursor_id=request.cursor_id,
                use_existing_cursor=True,
                after_key=advance_to,
                snapshot_id=request.snapshot.snapshot_id,
                note="token 边界已确认，跳过已交付范围，从确认高水位之后继续",
            ),
            duplicate_risk=DuplicateRisk(
                level=RiskLevel.NONE,
                detail="唯一排序键且确认范围连续，跳过的行不会再返回",
            ),
            discarded_ranges=(skipped,),
            checkpoint=checkpoint,
            safe_to_splice=True,
            reason="快照有效且 token 边界位于已确认范围内",
            warnings=tuple(warnings),
        )

    warnings = []
    risk_level = RiskLevel.NONE
    risk_detail = "唯一排序键下边界行之后的行尚未交付，按 token 续读不会重复"
    if token_replayed:
        risk_level = RiskLevel.POSSIBLE_PAGE
        risk_detail = (
            "token %s 已被消费过，边界之后的页可能已交付，调用方需按唯一键去重"
            % token.token_id
        )
        warnings.append("重复 token 且边界尚未确认，无法判断页内哪些行已送达")

    return RecoveryPlan(
        method=RecoveryMethod.RESUME,
        next_cursor=NextCursor(
            cursor_id=request.cursor_id,
            use_existing_cursor=True,
            after_key=token.boundary_key,
            snapshot_id=request.snapshot.snapshot_id,
            note="沿用服务端游标，从 resume token 边界之后继续读取",
        ),
        duplicate_risk=DuplicateRisk(level=risk_level, detail=risk_detail),
        discarded_ranges=(),
        checkpoint=checkpoint,
        safe_to_splice=risk_level == RiskLevel.NONE,
        reason="快照有效、排序键唯一，token 边界可作为精确断点",
        warnings=tuple(warnings),
    )


def _merge_ranges(ranges):
    """合并重叠或首尾相接（半开区间 end == next start）的行范围。"""
    ordered = sorted(
        ranges,
        key=lambda item: (item.start_key is not None, item.start_key or ()),
    )
    merged = []
    for row_range in ordered:
        if not merged:
            merged.append(row_range)
            continue
        previous = merged[-1]
        if row_range.start_key is not None and row_range.start_key > previous.end_key:
            merged.append(row_range)
            continue
        if row_range.end_key <= previous.end_key:
            continue
        overlaps = row_range.start_key is not None and row_range.start_key < previous.end_key
        merged[-1] = RowRange(
            start_key=previous.start_key,
            end_key=row_range.end_key,
            row_count=previous.row_count + (0 if overlaps else row_range.row_count),
        )
    return merged


def _contiguous_checkpoint(merged):
    """取从头开始的连续确认前缀作为确认点。"""
    if not merged or merged[0].start_key is not None:
        return Checkpoint(key=None, row_count=0, contiguous=False)
    head = merged[0]
    contiguous = len(merged) == 1
    return Checkpoint(key=head.end_key, row_count=head.row_count, contiguous=contiguous)


def _range_covering(ranges, start_key, end_key):
    """判断 ``[start_key, end_key)`` 是否完全落在某个已确认范围之内。"""
    for row_range in ranges:
        begins_ok = row_range.start_key is None or (
            start_key is not None and row_range.start_key <= start_key
        )
        if begins_ok and row_range.end_key >= end_key:
            return True
    return False

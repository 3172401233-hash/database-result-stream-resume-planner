"""结果流恢复规划器的单元测试。

所有用例都使用内存中的值对象，不连接任何真实数据库。
运行方式：python -m unittest
"""

import unittest

from stream_recovery import (
    Checkpoint,
    InvalidResumeTokenError,
    PaginationBoundary,
    QuerySnapshot,
    RecoveryMethod,
    RecoveryRequest,
    ReplicaPosition,
    ResumeToken,
    RiskLevel,
    RowRange,
    SortDirection,
    SortKey,
    SnapshotState,
    TransactionValidity,
    plan_recovery,
)


NOW = 1_000_000
DEADLINE = NOW + 60_000


def make_token(
    token_id="token-1",
    boundary=(10,),
    snapshot_id="snap-A",
    generation=1,
    cursor_id="cursor-1",
    page_sequence=3,
):
    return ResumeToken(
        token_id=token_id,
        cursor_id=cursor_id,
        snapshot_id=snapshot_id,
        generation=generation,
        boundary_key=boundary,
        page_sequence=page_sequence,
    )


def make_request(
    acknowledged_ranges=(),
    sort_key=None,
    token=None,
    snapshot_state=SnapshotState.ACTIVE,
    snapshot_id="snap-A",
    generation=1,
    deadline=DEADLINE,
    now=NOW,
    current_version=100,
    required_version=100,
    consumed_token_ids=(),
    boundary_last_key=(100,),
    cursor_id="cursor-1",
):
    if sort_key is None:
        sort_key = SortKey(columns=("id",), unique=True)
    if token is None:
        token = make_token(cursor_id=cursor_id)
    return RecoveryRequest(
        cursor_id=cursor_id,
        snapshot=QuerySnapshot(
            snapshot_id=snapshot_id,
            state=snapshot_state,
            generation=generation,
        ),
        acknowledged_ranges=tuple(acknowledged_ranges),
        sort_key=sort_key,
        pagination_boundary=PaginationBoundary(last_key=boundary_last_key),
        transaction=TransactionValidity(deadline_utc=deadline, now_utc=now),
        replica=ReplicaPosition(
            replica_id="replica-1",
            current_version=current_version,
            required_version=required_version,
        ),
        resume_token=token,
        consumed_token_ids=tuple(consumed_token_ids),
    )


class NetworkInterruptionTests(unittest.TestCase):
    """网络中断后用旧 token 重连的基础恢复路径。"""

    def test_clean_resume_from_token_boundary(self):
        request = make_request(acknowledged_ranges=())

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.RESUME)
        self.assertTrue(plan.next_cursor.use_existing_cursor)
        self.assertEqual(plan.next_cursor.after_key, (10,))
        self.assertEqual(plan.next_cursor.snapshot_id, "snap-A")
        self.assertEqual(plan.duplicate_risk.level, RiskLevel.NONE)
        self.assertTrue(plan.safe_to_splice)
        self.assertEqual(plan.discarded_ranges, ())
        self.assertEqual(plan.checkpoint, Checkpoint(key=None, row_count=0, contiguous=False))

    def test_acknowledged_prefix_advances_cursor_without_duplicates(self):
        request = make_request(
            acknowledged_ranges=(
                RowRange(start_key=None, end_key=(10,), row_count=10),
                RowRange(start_key=(10,), end_key=(25,), row_count=15),
            )
        )

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.RESUME_ADVANCED)
        self.assertEqual(plan.next_cursor.after_key, (25,))
        self.assertEqual(plan.duplicate_risk.level, RiskLevel.NONE)
        self.assertTrue(plan.safe_to_splice)
        self.assertEqual(len(plan.discarded_ranges), 1)
        discarded = plan.discarded_ranges[0]
        self.assertEqual(discarded.start_key, (10,))
        self.assertEqual(discarded.end_key, (25,))
        self.assertEqual(discarded.row_count, 25)
        self.assertEqual(plan.checkpoint.key, (25,))
        self.assertEqual(plan.checkpoint.row_count, 25)
        self.assertTrue(plan.checkpoint.contiguous)

    def test_unacknowledged_gap_keeps_token_as_exact_breakpoint(self):
        # 已确认范围从头连续到 8，但 token 边界 10 尚未确认，
        # 唯一键下仍可从 token 精确续读。
        request = make_request(
            acknowledged_ranges=(
                RowRange(start_key=None, end_key=(8,), row_count=8),
            )
        )

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.RESUME)
        self.assertEqual(plan.next_cursor.after_key, (10,))
        self.assertEqual(plan.duplicate_risk.level, RiskLevel.NONE)
        self.assertEqual(plan.checkpoint.key, (8,))


class SnapshotInvalidationTests(unittest.TestCase):
    """快照失效时必须重新查询，禁止拼接旧结果。"""

    def test_expired_snapshot_forces_requery(self):
        request = make_request(
            snapshot_state=SnapshotState.EXPIRED,
            acknowledged_ranges=(
                RowRange(start_key=None, end_key=(10,), row_count=10),
            ),
        )

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.REQUERY)
        self.assertFalse(plan.next_cursor.use_existing_cursor)
        self.assertIsNone(plan.next_cursor.after_key)
        self.assertEqual(plan.duplicate_risk.level, RiskLevel.FULL_STREAM)
        self.assertFalse(plan.safe_to_splice)
        self.assertEqual(len(plan.discarded_ranges), 1)
        self.assertEqual(plan.discarded_ranges[0].end_key, (10,))
        self.assertIn("快照已过期", plan.reason)
        self.assertIn("禁止将旧快照结果与新快照结果直接拼接", plan.warnings)

    def test_transaction_deadline_passed_forces_requery(self):
        request = make_request(now=DEADLINE + 1)

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.REQUERY)
        self.assertFalse(plan.safe_to_splice)
        self.assertIn("事务已超过有效期", plan.reason)

    def test_terminated_transaction_forces_requery(self):
        request = make_request(deadline=None)

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.REQUERY)
        self.assertIn("事务已终止", plan.reason)

    def test_replica_purged_snapshot_version_forces_requery(self):
        request = make_request(current_version=101, required_version=100)

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.REQUERY)
        self.assertIn("旧版本被清理", plan.reason)

    def test_stale_token_pointing_at_old_snapshot_forces_requery(self):
        token = make_token(snapshot_id="snap-old", generation=0)
        request = make_request(token=token)

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.REQUERY)
        self.assertIn("跨快照不能拼接", plan.reason)
        self.assertFalse(plan.safe_to_splice)

    def test_unavailable_snapshot_state_forces_requery(self):
        request = make_request(snapshot_state=SnapshotState.UNAVAILABLE)

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.REQUERY)
        self.assertIn("不可用", plan.reason)


class NonUniqueSortKeyTests(unittest.TestCase):
    """排序键重复时不能假装可以精确恢复。"""

    def test_duplicate_sort_key_rereads_tie_group(self):
        sort_key = SortKey(
            columns=("status", "created_at"),
            unique=False,
            tie_group_value=("PENDING", 55),
        )
        token = make_token(boundary=("PENDING", 55))
        request = make_request(sort_key=sort_key, token=token, boundary_last_key=("PENDING", 99))

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.REREAD_TIE_GROUP)
        self.assertEqual(plan.duplicate_risk.level, RiskLevel.TIE_GROUP)
        self.assertFalse(plan.safe_to_splice)
        self.assertEqual(plan.next_cursor.after_key, ("PENDING", 55))
        self.assertTrue(plan.next_cursor.use_existing_cursor)
        discarded = plan.discarded_ranges[0]
        self.assertEqual(discarded.start_key, discarded.end_key)
        self.assertIn("不能保证精确恢复", plan.warnings[0])
        self.assertIn("去重", plan.duplicate_risk.detail)

    def test_tie_group_falls_back_to_token_boundary_when_group_value_missing(self):
        sort_key = SortKey(columns=("category",), unique=False)
        token = make_token(boundary=("A",))
        request = make_request(sort_key=sort_key, token=token, boundary_last_key=("Z",))

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.REREAD_TIE_GROUP)
        self.assertEqual(plan.next_cursor.after_key, ("A",))


class ReplayedTokenTests(unittest.TestCase):
    """客户端重复提交同一个 resume token。"""

    def test_replayed_token_with_acknowledged_boundary_advances_safely(self):
        request = make_request(
            acknowledged_ranges=(
                RowRange(start_key=None, end_key=(10,), row_count=10),
                RowRange(start_key=(10,), end_key=(30,), row_count=20),
            ),
            consumed_token_ids=("token-1",),
        )

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.RESUME_ADVANCED)
        self.assertEqual(plan.next_cursor.after_key, (30,))
        self.assertEqual(plan.duplicate_risk.level, RiskLevel.NONE)
        self.assertTrue(plan.safe_to_splice)
        self.assertTrue(any("重复提交 token" in warning for warning in plan.warnings))

    def test_replayed_token_without_acknowledgement_flags_page_risk(self):
        request = make_request(consumed_token_ids=("token-1",))

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.RESUME)
        self.assertEqual(plan.duplicate_risk.level, RiskLevel.POSSIBLE_PAGE)
        self.assertFalse(plan.safe_to_splice)
        self.assertEqual(plan.next_cursor.after_key, (10,))
        self.assertIn("去重", plan.duplicate_risk.detail)

    def test_replayed_token_on_non_unique_key_still_rereads_group(self):
        sort_key = SortKey(columns=("status",), unique=False, tie_group_value=("PENDING",))
        token = make_token(boundary=("PENDING",))
        request = make_request(
            sort_key=sort_key,
            token=token,
            boundary_last_key=("PENDING",),
            consumed_token_ids=("token-1",),
        )

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.REREAD_TIE_GROUP)
        self.assertEqual(plan.duplicate_risk.level, RiskLevel.TIE_GROUP)
        self.assertTrue(any("已被消费过" in warning for warning in plan.warnings))


class TokenValidationTests(unittest.TestCase):
    """resume token 与读取上下文不一致时快速失败。"""

    def test_cursor_mismatch_raises(self):
        token = make_token(cursor_id="cursor-other")
        request = make_request(token=token)

        with self.assertRaises(InvalidResumeTokenError) as ctx:
            plan_recovery(request)
        self.assertIn("不匹配", ctx.exception.reason)

    def test_token_beyond_pagination_boundary_raises(self):
        request = make_request(
            token=make_token(boundary=(999,)),
            boundary_last_key=(100,),
        )

        with self.assertRaises(InvalidResumeTokenError):
            plan_recovery(request)

    def test_desc_sort_direction_rejected(self):
        sort_key = SortKey(columns=("id",), direction=SortDirection.DESC)
        request = make_request(sort_key=sort_key)

        with self.assertRaises(InvalidResumeTokenError):
            plan_recovery(request)


class RangeBookkeepingTests(unittest.TestCase):
    """确认范围合并与确认点计算。"""

    def test_overlapping_ranges_are_not_double_counted(self):
        request = make_request(
            acknowledged_ranges=(
                RowRange(start_key=None, end_key=(20,), row_count=20),
                RowRange(start_key=(10,), end_key=(30,), row_count=20),
            )
        )

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.RESUME_ADVANCED)
        self.assertEqual(plan.checkpoint.key, (30,))
        self.assertEqual(plan.checkpoint.row_count, 20)

    def test_disjoint_ranges_mark_non_contiguous_checkpoint(self):
        request = make_request(
            acknowledged_ranges=(
                RowRange(start_key=None, end_key=(5,), row_count=5),
                RowRange(start_key=(20,), end_key=(30,), row_count=10),
            )
        )

        plan = plan_recovery(request)

        self.assertEqual(plan.method, RecoveryMethod.RESUME)
        self.assertEqual(plan.checkpoint.key, (5,))
        self.assertFalse(plan.checkpoint.contiguous)

    def test_invalid_row_range_rejected(self):
        with self.assertRaises(ValueError):
            RowRange(start_key=(10,), end_key=(5,))


if __name__ == "__main__":
    unittest.main()

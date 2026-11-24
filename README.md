# database-result-stream-resume-planner

大型查询通过服务端游标流式返回结果。网络中断后，客户端拿着旧的
resume token 重连时可能面对三种问题：事务快照已失效、分页边界上的行
已经交付过、排序键不唯一导致无法定位精确断点。

本项目实现一个**纯内存的结果流恢复规划器**：根据调用方传入的游标 ID、
查询快照、已确认行范围、排序键、分页边界、事务有效期、副本位置和
resume token，计算下一次读取应当从哪里继续，并给出重复风险、需要
丢弃的范围和确认点。

## 重要边界：不连接真实数据库

- 规划器**不会**建立数据库连接、不会执行 SQL、不会发起任何网络请求。
- 所有输入都是调用方自行采集后传入的不可变值对象（见 `stream_recovery/models.py`）。
- 输出只是一份“恢复计划”，由调用方决定如何在真实数据库驱动上执行。
- 测试同样完全运行在内存中，使用 `python -m unittest` 即可，无需任何外部服务。

## 恢复决策表

| 条件 | 恢复方式 | 重复风险 | 能否拼接旧结果 |
| --- | --- | --- | --- |
| 快照有效 + 排序键唯一 + token 边界未确认 | `RESUME` | `NONE`（重复 token 时为 `POSSIBLE_PAGE`） | 是 |
| 快照有效 + 排序键唯一 + token 边界已确认 | `RESUME_ADVANCED`，推进到确认高水位 | `NONE` | 是 |
| 快照有效 + 排序键不唯一 | `REREAD_TIE_GROUP`，重读边界等值分组 | `TIE_GROUP` | 否，调用方去重 |
| 快照过期 / 事务失效 / 副本清理旧版本 / token 指向旧快照 | `REQUERY`，全新快照重新查询 | `FULL_STREAM` | 否，禁止拼接 |

排序键不唯一时，规划器**不会**伪造一个精确断点，而是明确返回
`REREAD_TIE_GROUP` 并要求调用方按业务键去重。

## 输入与输出

输入 `RecoveryRequest` 包含：

- `cursor_id`：游标 ID；resume token 的游标必须与之匹配，否则抛出
  `InvalidResumeTokenError`。
- `snapshot`：查询快照 ID、世代（generation）和状态。
- `acknowledged_ranges`：客户端已确认的半开行范围 `[start_key, end_key)`，
  会自动合并重叠/相接范围并计算连续确认前缀。
- `sort_key`：排序列、方向、是否唯一，以及非唯一键时的等值分组键值。
- `pagination_boundary`：服务端游标当前的最后一条排序键。
- `transaction`：事务有效期（UTC 时间戳），到期即判定快照失效。
- `replica`：副本回放位置，快照所需版本被清理时判定快照失效。
- `resume_token` 与 `consumed_token_ids`：旧 token 及已经消费过的 token 列表，
  用于识别重复提交。

输出 `RecoveryPlan` 包含：

- `method`：`RESUME` / `RESUME_ADVANCED` / `REREAD_TIE_GROUP` / `REQUERY`。
- `next_cursor`：下一游标位置（是否沿用旧游标、`after_key`、快照 ID）。
- `duplicate_risk`：风险等级与人类可读说明。
- `discarded_ranges`：本次恢复中跳过或必须弃用的行范围。
- `checkpoint`：连续确认前缀的高水位确认点。
- `safe_to_splice`：能否把新旧结果直接拼接；快照失效与非唯一键场景恒为 `False`。
- `reason` / `warnings`：决策依据与去重提示。

## 使用示例

```python
from stream_recovery import (
    PaginationBoundary, QuerySnapshot, RecoveryRequest, ReplicaPosition,
    ResumeToken, RowRange, SnapshotState, SortKey, TransactionValidity,
    plan_recovery,
)

request = RecoveryRequest(
    cursor_id="cursor-1",
    snapshot=QuerySnapshot("snap-A", SnapshotState.ACTIVE, generation=1),
    acknowledged_ranges=(
        RowRange(start_key=None, end_key=(10,), row_count=10),
        RowRange(start_key=(10,), end_key=(25,), row_count=15),
    ),
    sort_key=SortKey(columns=("id",), unique=True),
    pagination_boundary=PaginationBoundary(last_key=(100,)),
    transaction=TransactionValidity(deadline_utc=1_060_000, now_utc=1_000_000),
    replica=ReplicaPosition("replica-1", current_version=100, required_version=100),
    resume_token=ResumeToken(
        token_id="token-1", cursor_id="cursor-1", snapshot_id="snap-A",
        generation=1, boundary_key=(10,), page_sequence=3,
    ),
)

plan = plan_recovery(request)
print(plan.method)               # RecoveryMethod.RESUME_ADVANCED
print(plan.next_cursor.after_key)  # (25,)
print(plan.duplicate_risk.level)   # RiskLevel.NONE
```

## 目录结构

```
stream_recovery/
    __init__.py        # 对外 API
    exceptions.py      # InvalidResumeTokenError
    models.py          # 输入/输出值对象与枚举
    planner.py         # 纯函数规划逻辑
tests/
    test_recovery_planner.py
```

## 运行测试

```bash
python -m unittest
```

测试覆盖：网络中断干净续读、确认前缀推进、事务过期、快照过期/不可用、
副本清理旧版本、token 指向旧快照、排序键重复（等值分组重读）、
重复 token（已确认/未确认/非唯一键）、游标不匹配与非法边界。

## Python 版本

代码仅使用标准库（`dataclasses`、`enum`、`typing`），语法兼容
Python 3.8.2，不依赖任何第三方包。

"""结果流恢复规划器的领域模型。

所有模型都是不可变的值对象，规划过程只读取这些对象，
不会发起任何数据库或网络调用。
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Tuple


class RecoveryMethod(str, Enum):
    """规划器给出的恢复方式。"""

    RESUME = "RESUME"
    """快照有效、排序键唯一：从 token 边界之后继续精确读取。"""

    RESUME_ADVANCED = "RESUME_ADVANCED"
    """token 边界已被确认，推进到确认高水位再继续，不重复读取。"""

    REREAD_TIE_GROUP = "REREAD_TIE_GROUP"
    """无唯一排序键：重读边界上的等值分组，由调用方按行去重。"""

    REQUERY = "REQUERY"
    """快照失效：用全新快照重新查询，旧结果只允许用于去重，禁止拼接。"""


class RiskLevel(str, Enum):
    """重复风险等级。"""

    NONE = "NONE"
    POSSIBLE_PAGE = "POSSIBLE_PAGE"
    TIE_GROUP = "TIE_GROUP"
    FULL_STREAM = "FULL_STREAM"


class SortDirection(str, Enum):
    ASC = "ASC"
    DESC = "DESC"


class SnapshotState(str, Enum):
    """服务端快照当前的状态。"""

    ACTIVE = "ACTIVE"
    EXPIRED = "EXPIRED"
    UNAVAILABLE = "UNAVAILABLE"


@dataclass(frozen=True)
class SortKey:
    """排序键描述。

    ``columns`` 为排序列名（按优先级排列）；``unique`` 表示该排序键
    能否唯一确定一行（例如包含主键）。``tie_group_value`` 是边界行的
    排序键值，用于在非唯一键场景定位需要重读的等值分组。
    """

    columns: Tuple[str, ...]
    direction: SortDirection = SortDirection.ASC
    unique: bool = True
    tie_group_value: Optional[Tuple[Any, ...]] = None


@dataclass(frozen=True)
class RowRange:
    """一个半开的行范围 ``[start_key, end_key)``。

    键值使用排序键的元组表示，元组之间可直接比较；
    ``start_key`` 为 ``None`` 表示范围从头开始。
    """

    start_key: Optional[Tuple[Any, ...]]
    end_key: Tuple[Any, ...]
    row_count: int = 0

    def __post_init__(self):
        if self.start_key is not None and self.start_key > self.end_key:
            raise ValueError("行范围起点不能晚于终点: %r > %r" % (self.start_key, self.end_key))
        if self.row_count < 0:
            raise ValueError("行范围行数不能为负数")


@dataclass(frozen=True)
class QuerySnapshot:
    """查询快照标识与服务端状态。"""

    snapshot_id: str
    state: SnapshotState
    generation: int


@dataclass(frozen=True)
class TransactionValidity:
    """事务有效期。``deadline_utc`` 为 None 表示事务已提交/已终止。"""

    deadline_utc: Optional[int]
    now_utc: int

    @property
    def alive(self):
        return self.deadline_utc is not None and self.now_utc < self.deadline_utc


@dataclass(frozen=True)
class ReplicaPosition:
    """副本回放位置。

    恢复要求副本仍保留快照所需的最早版本 ``required_version``。
    """

    replica_id: str
    current_version: int
    required_version: int

    @property
    def retains_snapshot(self):
        return self.current_version <= self.required_version


@dataclass(frozen=True)
class PaginationBoundary:
    """服务端游标当前可见的分页边界（最后一条已产出的排序键）。"""

    last_key: Tuple[Any, ...]
    has_more: bool = True


@dataclass(frozen=True)
class ResumeToken:
    """客户端断线重连时携带的旧 resume token。"""

    token_id: str
    cursor_id: str
    snapshot_id: str
    generation: int
    boundary_key: Tuple[Any, ...]
    page_sequence: int


@dataclass(frozen=True)
class NextCursor:
    """规划器计算出的下一次读取游标位置。"""

    cursor_id: str
    use_existing_cursor: bool
    after_key: Optional[Tuple[Any, ...]]
    snapshot_id: str
    note: str = ""


@dataclass(frozen=True)
class DuplicateRisk:
    """重复风险描述。"""

    level: RiskLevel
    detail: str


@dataclass(frozen=True)
class Checkpoint:
    """确认点：连续确认前缀的高水位。"""

    key: Optional[Tuple[Any, ...]]
    row_count: int
    contiguous: bool


@dataclass(frozen=True)
class RecoveryPlan:
    """规划结果。"""

    method: RecoveryMethod
    next_cursor: NextCursor
    duplicate_risk: DuplicateRisk
    discarded_ranges: Tuple[RowRange, ...]
    checkpoint: Checkpoint
    safe_to_splice: bool
    reason: str
    warnings: Tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RecoveryRequest:
    """规划器的完整输入。"""

    cursor_id: str
    snapshot: QuerySnapshot
    acknowledged_ranges: Tuple[RowRange, ...]
    sort_key: SortKey
    pagination_boundary: PaginationBoundary
    transaction: TransactionValidity
    replica: ReplicaPosition
    resume_token: ResumeToken
    consumed_token_ids: Tuple[str, ...] = field(default_factory=tuple)

    def to_diagnostic_dict(self):
        """供日志使用的精简视图（不包含行数据本身）。"""
        return {
            "cursor_id": self.cursor_id,
            "snapshot_id": self.snapshot.snapshot_id,
            "token_id": self.resume_token.token_id,
            "sort_columns": list(self.sort_key.columns),
            "sort_unique": self.sort_key.unique,
        }

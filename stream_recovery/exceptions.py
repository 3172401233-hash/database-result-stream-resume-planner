"""恢复规划器的异常定义。"""


class InvalidResumeTokenError(ValueError):
    """resume token 与本次读取上下文不一致，无法用于规划。"""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason

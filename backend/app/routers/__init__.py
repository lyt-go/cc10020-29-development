"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import multi_transform, pipeline, resource

# multi_transform 含 /api/multi-transform/... 固定前缀，需在通用资源路由
# （含 /api/{module}/{entry_id} 动态路径）之前注册，避免路径被抢先匹配。
ROUTERS = [multi_transform, pipeline, resource]

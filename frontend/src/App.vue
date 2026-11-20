<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">API集成与事件编排平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向第三方API接入、Webhook事件接收、消息队列消费与服务编排的全链路API数据集成平台。</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "API端点注册", path: "/api_endpoint" }, { label: "Webhook通道", path: "/webhook_channel" }, { label: "事件结构定义", path: "/event_schema" }, { label: "消息队列消费", path: "/mq_consumer" }, { label: "死信队列管理", path: "/dlq_handler" }, { label: "认证凭证管理", path: "/api_auth_store" }, { label: "流量控制", path: "/rate_limit_config" }, { label: "数据转换编排", path: "/data_transform" }, { label: "多规则转换数据集", path: "/multi-transform-datasets" }, { label: "事件分发路由", path: "/dispatch_rule" }, { label: "事件去重", path: "/dedup_filter" }, { label: "批量接入调度", path: "/batch_ingest" }, { label: "数据落湖", path: "/data_lake_sink" }, { label: "接入质量校验", path: "/data_quality_rule" }, { label: "调用链路追踪", path: "/call_trace_log" }, { label: "增量水位管理", path: "/incre_cursor" }, { label: "历史回补任务", path: "/backfill_job" }, { label: "事件归档清理", path: "/event_archive" }, { label: "集成监控看板", path: "/integration_monitor" }]
</script>

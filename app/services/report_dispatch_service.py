"""报告订阅分发：把到期的订阅生成为报告并记录分发结果。

当前是**最小可用实现**：不接邮件/短信网关，只做「找待发送订阅 → 生成报告
→ 更新订阅的发送时间 → 把分发信息写入报告与日志」四步，因此同一订阅靠
订阅表自己的发送时间字段做节流（见 ReportSubscription.get_pending_subscriptions）。

入口 dispatch_pending_subscriptions 不抛异常：单订阅失败与整体异常都收敛成
`{success, dispatched, failed, results}` 结构返回，便于前端直接展示。
"""

from typing import Any, Dict, List, Optional

from loguru import logger

from app.models.realtime_report import ReportSubscription, RealtimeReport
from app.services.realtime_report_generator import RealtimeReportGenerator


class ReportDispatchService:
    """最小可用的报告订阅分发服务。

    当前阶段不强依赖外部邮件或短信网关，只完成：
    - 查找待发送订阅
    - 生成报告
    - 更新订阅发送时间
    - 将分发信息写入报告数据与服务日志
    """

    def __init__(self, generator: Optional[RealtimeReportGenerator] = None):
        self.generator = generator or RealtimeReportGenerator()

    def dispatch_pending_subscriptions(self) -> Dict[str, Any]:
        """扫描待推送的订阅并逐个派发，返回成功/失败计数与逐条结果。

        单条订阅失败不影响其余；顶层异常也被兜住返回 success=False ——
        调用方是定时任务，异常抛出去只会变成无信息的调度错误。
        """
        try:
            subscriptions = ReportSubscription.get_pending_subscriptions()
            results: List[Dict[str, Any]] = []

            for subscription in subscriptions:
                dispatch_result = self._dispatch_subscription(subscription)
                results.append(dispatch_result)

            return {
                "success": True,
                "dispatched": len([item for item in results if item.get("success")]),
                "failed": len([item for item in results if not item.get("success")]),
                "results": results,
            }
        except Exception as exc:
            logger.error(f"分发订阅失败: {exc}")
            return {"success": False, "message": str(exc), "dispatched": 0, "failed": 0, "results": []}

    def _dispatch_subscription(self, subscription: ReportSubscription) -> Dict[str, Any]:
        """派发生成并发送单条订阅。

        模板不存在或生成失败都返回失败结构而不抛异常（调用方按批处理，一条失败不该中断整批）；
        生成参数取自订阅的 schedule_config.parameters，让同一模板按订阅产出不同内容。
        """
        template = subscription.template
        if template is None:
            return {"success": False, "subscription_id": subscription.id, "message": "模板不存在"}

        parameters = {}
        schedule_config = subscription.schedule_config if isinstance(subscription.schedule_config, dict) else {}
        if isinstance(schedule_config, dict):
            parameters.update(schedule_config.get("parameters") or {})

        result = self.generator.generate_report(
            report_type=template.template_type,
            template_id=template.id,
            report_name=None,
            parameters=parameters,
            generated_by="subscription_dispatch",
        )
        if not result.get("success"):
            return {
                "success": False,
                "subscription_id": subscription.id,
                "message": result.get("message", "生成报告失败"),
            }

        report_id = result.get("data", {}).get("report_id")
        report = RealtimeReport.get_by_id(report_id) if report_id else None
        if report is not None:
            channels = subscription.notification_channels if isinstance(subscription.notification_channels, list) else ["log"]
            report.attach_dispatch_metadata(
                subscription_id=subscription.id,
                channels=channels,
                subscriber_email=subscription.subscriber_email,
                subscriber_phone=subscription.subscriber_phone,
            )

        subscription.update_send_time()

        logger.info(f"订阅 {subscription.id} 分发完成")
        return {
            "success": True,
            "subscription_id": subscription.id,
            "report_id": report_id,
            "message": "分发完成",
        }

import datetime
import re
import traceback
from pathlib import Path
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.download import DownloadChain
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.context import MediaInfo, TorrentInfo, Context
from app.core.metainfo import MetaInfo
from app.helper.rss import RssHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import ExistMediaInfo
from app.schemas.types import SystemConfigKey, MediaType

lock = Lock()


class SatoshiRss(_PluginBase):
    plugin_name = "订阅-Satoshi"
    plugin_desc = "定时刷新RSS报文，识别内容后添加订阅或直接下载。"
    plugin_icon = "customsubscribe.webp"
    plugin_version = "1.2"
    plugin_author = "wwhsaber"
    author_url = "https://github.com/wwhsaber"
    plugin_config_prefix = "satoshirss_"
    plugin_order = 19
    auth_level = 2

    _scheduler: Optional[BackgroundScheduler] = None
    _cache_path: Optional[Path] = None

    _enabled: bool = False
    _cron: str = ""
    _notify: bool = False
    _onlyonce: bool = False
    _rss_url: str = ""
    _address: str = ""
    _include: str = ""
    _exclude: str = ""
    _proxy: bool = False
    _filter: bool = False
    _clear: bool = False
    _clearflag: bool = False
    _action: str = "subscribe"
    _save_path: str = ""
    _size_range: str = ""

    def init_plugin(self, config: dict = None):
        self.stop_service()

        need_save = False
        if config:
            self.__validate_and_fix_config(config=config)

            rss_urls = self.__split_rss_urls(config.get("address"))
            current_input = self.__clean_text(config.get("rss_url"))
            if current_input:
                need_save = True
                if current_input not in rss_urls:
                    rss_urls.append(current_input)

            self._enabled = bool(config.get("enabled"))
            self._cron = config.get("cron") or ""
            self._notify = bool(config.get("notify"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._rss_url = ""
            self._address = "\n".join(rss_urls)
            self._include = config.get("include") or ""
            self._exclude = config.get("exclude") or ""
            self._proxy = bool(config.get("proxy"))
            self._filter = bool(config.get("filter"))
            self._clear = bool(config.get("clear"))
            self._action = config.get("action") or "subscribe"
            self._save_path = config.get("save_path") or ""
            self._size_range = config.get("size_range") or ""
            self.__prune_removed_urls(rss_urls)

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info("自定义订阅服务启动，立即运行一次")
            self._scheduler.add_job(
                func=self.check,
                trigger="date",
                run_date=datetime.datetime.now(
                    tz=pytz.timezone(settings.TZ)
                ) + datetime.timedelta(seconds=3),
            )

            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

        if self._onlyonce or self._clear:
            self._onlyonce = False
            self._clearflag = self._clear
            self._clear = False
            need_save = True

        if need_save:
            self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": "删除RSS详情记录",
            },
            {
                "path": "/refresh_rss",
                "endpoint": self.refresh_rss,
                "methods": ["GET"],
                "summary": "手动刷新RSS详情",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [
                {
                    "id": "RssSubscribe",
                    "name": "自定义订阅服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.check,
                    "kwargs": {},
                }
            ]
        if self._enabled:
            return [
                {
                    "id": "RssSubscribe",
                    "name": "自定义订阅服务",
                    "trigger": "interval",
                    "func": self.check,
                    "kwargs": {"minutes": 30},
                }
            ]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify",
                                            "label": "发送通知",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "5位cron表达式，留空自动",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "action",
                                            "label": "动作",
                                            "items": [
                                                {"title": "订阅", "value": "subscribe"},
                                                {"title": "下载", "value": "download"},
                                            ],
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 9},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "rss_url",
                                            "label": "新增RSS地址",
                                            "placeholder": "输入单个RSS地址，保存后自动加入下方列表",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VBtn",
                                        "props": {
                                            "block": True,
                                            "color": "primary",
                                            "variant": "tonal",
                                        },
                                        "text": "详情",
                                        "events": {
                                            "click": {
                                                "api": "plugin/SatoshiRss/refresh_rss",
                                                "method": "get",
                                                "params": {
                                                    "apikey": settings.API_TOKEN
                                                },
                                            }
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "新增链接时先点页面保存。保存后会自动追加到下方列表；详情按钮会刷新列表里的全部RSS，并把成功或失败日志写到详情页。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VTextarea",
                                        "props": {
                                            "model": "address",
                                            "label": "已添加RSS列表",
                                            "rows": 4,
                                            "placeholder": "一行一个RSS地址，可直接编辑后保存",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "include",
                                            "label": "包含",
                                            "placeholder": "支持正则表达式",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "exclude",
                                            "label": "排除",
                                            "placeholder": "支持正则表达式",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "size_range",
                                            "label": "种子大小(GB)",
                                            "placeholder": "如：3 或 3-5",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "save_path",
                                            "label": "保存目录",
                                            "placeholder": "下载时有效，留空自动",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "proxy",
                                            "label": "使用代理服务器",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "filter",
                                            "label": "使用订阅优先级规则",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clear",
                                            "label": "清理历史记录",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "cron": "*/30 * * * *",
            "rss_url": "",
            "address": "",
            "include": "",
            "exclude": "",
            "proxy": False,
            "clear": False,
            "filter": False,
            "action": "subscribe",
            "save_path": "",
            "size_range": "",
        }

    def get_page(self) -> List[dict]:
        records = self.__read_detail_records()
        if not records:
            return [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {"class": "text-center"},
                }
            ]

        records = sorted(records, key=lambda item: item.get("last_time") or "", reverse=True)
        cards = []
        for record in records:
            url = record.get("url") or "未知RSS"
            items = record.get("items") or []
            logs = record.get("logs") or []

            item_blocks = []
            if items:
                for item in items:
                    item_content = [
                        {
                            "component": "VCardTitle",
                            "props": {"class": "text-body-1 break-words"},
                            "text": item.get("title") or "未命名条目",
                        },
                        {
                            "component": "VCardText",
                            "text": f"处理结果：{item.get('status_text') or '未处理'}",
                        },
                        {
                            "component": "VCardText",
                            "text": item.get("message") or "",
                        },
                    ]
                    if item.get("link"):
                        item_content.append(
                            {
                                "component": "div",
                                "props": {"class": "px-4 pb-2"},
                                "content": [
                                    {
                                        "component": "VBtn",
                                        "props": {
                                            "href": item.get("link"),
                                            "target": "_blank",
                                            "variant": "text",
                                            "size": "small",
                                        },
                                        "text": "打开条目链接",
                                    }
                                ],
                            }
                        )
                    if item.get("pubdate"):
                        item_content.append(
                            {
                                "component": "VCardText",
                                "text": f"发布时间：{item.get('pubdate')}",
                            }
                        )
                    if item.get("size_text"):
                        item_content.append(
                            {
                                "component": "VCardText",
                                "text": f"大小：{item.get('size_text')}",
                            }
                        )
                    if item.get("description"):
                        item_content.append(
                            {
                                "component": "VCardText",
                                "text": f"详情：{item.get('description')}",
                            }
                        )
                    item_blocks.append(
                        {
                            "component": "VCard",
                            "props": {"variant": "tonal", "class": "mx-4 mb-2"},
                            "content": item_content,
                        }
                    )
            else:
                item_blocks.append(
                    {
                        "component": "VCardText",
                        "text": "暂无RSS条目详情",
                    }
                )

            log_blocks = []
            if logs:
                for log_item in logs:
                    log_blocks.append(
                        {
                            "component": "VCardText",
                            "text": f"[{log_item.get('time')}] [{log_item.get('level')}] {log_item.get('message')}",
                        }
                    )
            else:
                log_blocks.append(
                    {
                        "component": "VCardText",
                        "text": "暂无执行日志",
                    }
                )

            cards.append(
                {
                    "component": "VCard",
                    "content": [
                        {
                            "component": "VDialogCloseBtn",
                            "props": {"innerClass": "absolute top-0 right-0"},
                            "events": {
                                "click": {
                                    "api": "plugin/SatoshiRss/delete_history",
                                    "method": "get",
                                    "params": {
                                        "key": url,
                                        "apikey": settings.API_TOKEN,
                                    },
                                }
                            },
                        },
                        {
                            "component": "VCardTitle",
                            "props": {"class": "pe-10 break-words"},
                            "text": url,
                        },
                        {
                            "component": "VCardText",
                            "text": f"最近执行：{record.get('last_time') or '-'}",
                        },
                        {
                            "component": "VCardText",
                            "text": f"状态：{record.get('status_text') or '-'}",
                        },
                        {
                            "component": "VCardText",
                            "text": record.get("message") or "",
                        },
                        {
                            "component": "div",
                            "props": {"class": "px-4 pb-2 d-flex flex-wrap ga-2"},
                            "content": [
                                {
                                    "component": "VBtn",
                                    "props": {
                                        "href": url,
                                        "target": "_blank",
                                        "variant": "text",
                                        "size": "small",
                                    },
                                    "text": "打开RSS链接",
                                }
                            ],
                        },
                        {"component": "VDivider"},
                        {
                            "component": "VCardText",
                            "text": f"条目总数：{record.get('item_total', 0)}，成功：{record.get('success_total', 0)}，跳过：{record.get('skip_total', 0)}，失败：{record.get('error_total', 0)}",
                        },
                        {
                            "component": "VCardText",
                            "text": "RSS条目详情",
                        },
                        *item_blocks,
                        {"component": "VDivider"},
                        {
                            "component": "VCardText",
                            "text": "执行日志",
                        },
                        *log_blocks,
                    ],
                }
            )

        return [
            {
                "component": "div",
                "props": {"class": "d-flex flex-column ga-3"},
                "content": cards,
            }
        ]

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as err:
            logger.error("退出插件失败：%s" % str(err))

    def refresh_rss(self, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")

        urls = self.__get_rss_urls()
        if not urls:
            return schemas.Response(success=False, message="请先保存至少一个RSS地址")

        if not lock.acquire(blocking=False):
            return schemas.Response(success=False, message="任务仍在执行中，请稍后再试")

        try:
            summary = self.__run_for_urls(urls=urls, trigger_source="manual")
        finally:
            lock.release()

        return schemas.Response(
            success=summary.get("failed_urls", 0) < summary.get("total_urls", 0),
            message=summary.get("message") or "刷新完成",
        )

    def delete_history(self, key: str, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")

        rss_urls = [url for url in self.__get_rss_urls() if url != key]
        history_items = [item for item in self.__read_history() if item.get("url") != key]
        detail_records = [item for item in self.__read_detail_records() if item.get("url") != key]

        self._address = "\n".join(rss_urls)
        self._rss_url = ""
        self.save_data("history", history_items)
        self.save_data("rss_detail_records", detail_records)
        self.__update_config()
        return schemas.Response(success=True, message="删除成功")

    def __update_config(self):
        self.update_config(
            {
                "enabled": self._enabled,
                "notify": self._notify,
                "onlyonce": self._onlyonce,
                "cron": self._cron,
                "rss_url": self._rss_url,
                "address": self._address,
                "include": self._include,
                "exclude": self._exclude,
                "proxy": self._proxy,
                "clear": self._clear,
                "filter": self._filter,
                "action": self._action,
                "save_path": self._save_path,
                "size_range": self._size_range,
            }
        )

    def check(self):
        urls = self.__get_rss_urls()
        if not urls:
            return

        if not lock.acquire(blocking=False):
            logger.info("自定义订阅任务仍在执行中，跳过本次运行")
            return

        try:
            self.__run_for_urls(urls=urls, trigger_source="schedule")
        finally:
            lock.release()

    def __run_for_urls(self, urls: List[str], trigger_source: str) -> Dict[str, Any]:
        if self._clearflag:
            history: List[dict] = []
            detail_map: Dict[str, dict] = {}
        else:
            history = self.__read_history()
            detail_map = {
                item.get("url"): item
                for item in self.__read_detail_records()
                if item.get("url")
            }

        processed_keys = {item.get("key") for item in history if item.get("key")}
        downloadchain = DownloadChain()
        subscribechain = SubscribeChain()
        filter_groups = self.systemconfig.get(SystemConfigKey.SubscribeFilterRuleGroups)

        total_urls = 0
        success_urls = 0
        failed_urls = 0

        for url in urls:
            total_urls += 1
            record = self.__handle_single_url(
                url=url,
                history=history,
                processed_keys=processed_keys,
                downloadchain=downloadchain,
                subscribechain=subscribechain,
                filter_groups=filter_groups,
                trigger_source=trigger_source,
            )
            detail_map[url] = record
            if record.get("status") == "error":
                failed_urls += 1
            else:
                success_urls += 1

        self.save_data("history", history)
        self.save_data("rss_detail_records", list(detail_map.values()))
        self._clearflag = False

        message = f"共处理 {total_urls} 个RSS，成功 {success_urls} 个，失败 {failed_urls} 个"
        return {
            "total_urls": total_urls,
            "success_urls": success_urls,
            "failed_urls": failed_urls,
            "message": message,
        }

    def __handle_single_url(
        self,
        url: str,
        history: List[dict],
        processed_keys: set,
        downloadchain: DownloadChain,
        subscribechain: SubscribeChain,
        filter_groups: Any,
        trigger_source: str,
    ) -> Dict[str, Any]:
        now_text = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logs: List[dict] = []
        items: List[dict] = []
        success_total = 0
        skip_total = 0
        error_total = 0

        self.__append_run_log(logs, "INFO", f"开始刷新RSS：{url}")
        results = RssHelper().parse(url, proxy=self._proxy)
        if results is None:
            self.__append_run_log(logs, "ERROR", "RSS链接已过期")
            return self.__build_detail_record(
                url=url,
                status="error",
                message="RSS链接已过期",
                last_time=now_text,
                trigger_source=trigger_source,
                success_total=0,
                skip_total=0,
                error_total=1,
                items=[],
                logs=logs,
            )

        if results is False:
            self.__append_run_log(logs, "ERROR", "获取RSS数据失败")
            return self.__build_detail_record(
                url=url,
                status="error",
                message="获取RSS数据失败",
                last_time=now_text,
                trigger_source=trigger_source,
                success_total=0,
                skip_total=0,
                error_total=1,
                items=[],
                logs=logs,
            )

        self.__append_run_log(logs, "INFO", f"读取到 {len(results)} 条RSS记录")
        if not results:
            self.__append_run_log(logs, "INFO", "RSS可访问，但没有可处理条目")

        for result in results:
            item_info = self.__build_item_info(result)
            try:
                item_result = self.__process_rss_item(
                    url=url,
                    result=result,
                    history=history,
                    processed_keys=processed_keys,
                    downloadchain=downloadchain,
                    subscribechain=subscribechain,
                    filter_groups=filter_groups,
                    logs=logs,
                )
                item_info.update(item_result)
            except Exception as err:
                item_info.update(
                    {
                        "status": "error",
                        "status_text": "处理失败",
                        "message": str(err),
                    }
                )
                self.__append_run_log(
                    logs,
                    "ERROR",
                    f"{item_info.get('title')} 处理异常：{err}",
                )
                logger.error(f"刷新RSS数据出错：{str(err)} - {traceback.format_exc()}")

            if item_info.get("status") == "success":
                success_total += 1
            elif item_info.get("status") == "skip":
                skip_total += 1
            else:
                error_total += 1
            items.append(item_info)

        if error_total and success_total:
            status = "partial"
        elif error_total and not success_total:
            status = "error"
        else:
            status = "success"

        message = (
            f"读取 {len(results)} 条，成功 {success_total} 条，跳过 {skip_total} 条，失败 {error_total} 条"
        )
        self.__append_run_log(logs, "INFO", f"RSS {url} 刷新完成")
        return self.__build_detail_record(
            url=url,
            status=status,
            message=message,
            last_time=now_text,
            trigger_source=trigger_source,
            success_total=success_total,
            skip_total=skip_total,
            error_total=error_total,
            items=items,
            logs=logs,
        )

    def __process_rss_item(
        self,
        url: str,
        result: dict,
        history: List[dict],
        processed_keys: set,
        downloadchain: DownloadChain,
        subscribechain: SubscribeChain,
        filter_groups: Any,
        logs: List[dict],
    ) -> Dict[str, Any]:
        title = result.get("title") or ""
        description = result.get("description") or ""
        enclosure = result.get("enclosure") or ""
        link = result.get("link") or ""
        size = result.get("size") or 0
        pubdate: datetime.datetime = result.get("pubdate")

        if not title:
            self.__append_run_log(logs, "WARNING", "存在标题为空的条目，已跳过")
            return {
                "status": "skip",
                "status_text": "已跳过",
                "message": "条目标题为空",
            }

        entry_key = self.__build_history_key(url=url, title=title, enclosure=enclosure, link=link)
        if entry_key in processed_keys:
            self.__append_run_log(logs, "INFO", f"{title} 已处理过，跳过")
            return {
                "status": "skip",
                "status_text": "已跳过",
                "message": "该条目已经处理过",
            }

        if self._include and not re.search(self._include, f"{title} {description}", re.IGNORECASE):
            self.__append_run_log(logs, "INFO", f"{title} 不符合包含规则")
            return {
                "status": "skip",
                "status_text": "已跳过",
                "message": "不符合包含规则",
            }

        if self._exclude and re.search(self._exclude, f"{title} {description}", re.IGNORECASE):
            self.__append_run_log(logs, "INFO", f"{title} 不符合排除规则")
            return {
                "status": "skip",
                "status_text": "已跳过",
                "message": "命中排除规则",
            }

        if self._size_range:
            sizes = [float(item) * 1024 ** 3 for item in self._size_range.split("-")]
            if len(sizes) == 1 and float(size) < sizes[0]:
                self.__append_run_log(logs, "INFO", f"{title} 种子大小不符合条件")
                return {
                    "status": "skip",
                    "status_text": "已跳过",
                    "message": "种子大小过小",
                }
            if len(sizes) > 1 and not sizes[0] <= float(size) <= sizes[1]:
                self.__append_run_log(logs, "INFO", f"{title} 种子大小不在指定范围")
                return {
                    "status": "skip",
                    "status_text": "已跳过",
                    "message": "种子大小不在指定范围",
                }

        meta = MetaInfo(title=title, subtitle=description)
        if not meta.name:
            self.__append_run_log(logs, "WARNING", f"{title} 未识别到有效数据")
            return {
                "status": "skip",
                "status_text": "已跳过",
                "message": "未识别到有效媒体信息",
            }

        mediainfo: MediaInfo = self.chain.recognize_media(meta=meta)
        if not mediainfo:
            self.__append_run_log(logs, "WARNING", f"{title} 未识别到媒体信息")
            return {
                "status": "skip",
                "status_text": "已跳过",
                "message": "未识别到媒体信息",
            }

        torrentinfo = TorrentInfo(
            title=title,
            description=description,
            enclosure=enclosure,
            page_url=link,
            size=size,
            pubdate=pubdate.strftime("%Y-%m-%d %H:%M:%S") if pubdate else None,
            site_proxy=self._proxy,
        )

        if self._filter:
            filtered = self.chain.filter_torrents(
                rule_groups=filter_groups,
                torrent_list=[torrentinfo],
                mediainfo=mediainfo,
            )
            if not filtered:
                self.__append_run_log(logs, "INFO", f"{title} 不匹配过滤规则")
                return {
                    "status": "skip",
                    "status_text": "已跳过",
                    "message": "不匹配过滤规则",
                }

        exist_info: Optional[ExistMediaInfo] = self.chain.media_exists(mediainfo=mediainfo)
        if mediainfo.type == MediaType.TV:
            if exist_info:
                exist_season = exist_info.seasons
                if exist_season:
                    exist_episodes = exist_season.get(meta.begin_season)
                    if exist_episodes and set(meta.episode_list).issubset(set(exist_episodes)):
                        self.__append_run_log(
                            logs,
                            "INFO",
                            f"{mediainfo.title_year} {meta.season_episode} 已存在",
                        )
                        return {
                            "status": "skip",
                            "status_text": "已跳过",
                            "message": "媒体库里已存在",
                        }
        elif exist_info:
            self.__append_run_log(logs, "INFO", f"{mediainfo.title_year} 已存在")
            return {
                "status": "skip",
                "status_text": "已跳过",
                "message": "媒体库里已存在",
            }

        action_message = ""
        if self._action == "download":
            download_hash, error_text = downloadchain.download_single(
                context=Context(
                    meta_info=meta,
                    media_info=mediainfo,
                    torrent_info=torrentinfo,
                ),
                save_path=self._save_path,
                username="RSS订阅",
                return_detail=True,
            )
            if not download_hash:
                action_message = error_text or "下载失败"
                self.__append_run_log(logs, "ERROR", f"{title} 下载失败：{action_message}")
                return {
                    "status": "error",
                    "status_text": "下载失败",
                    "message": action_message,
                }
            action_message = f"下载成功，任务ID：{download_hash}"
            self.__append_run_log(logs, "INFO", f"{title} 下载成功")
            success_text = "下载成功"
        else:
            if subscribechain.exists(mediainfo=mediainfo, meta=meta):
                self.__append_run_log(logs, "INFO", f"{mediainfo.title_year} 正在订阅中")
                return {
                    "status": "skip",
                    "status_text": "已跳过",
                    "message": "该媒体已经在订阅中",
                }

            subscribechain.add(
                title=mediainfo.title,
                year=mediainfo.year,
                mtype=mediainfo.type,
                tmdbid=mediainfo.tmdb_id,
                season=meta.begin_season,
                exist_ok=True,
                username="RSS订阅",
            )
            action_message = "订阅成功"
            self.__append_run_log(logs, "INFO", f"{title} 订阅成功")
            success_text = "订阅成功"

        history.append(
            {
                "title": f"{mediainfo.title} {meta.season}".strip(),
                "key": entry_key,
                "url": url,
                "type": mediainfo.type.value,
                "year": mediainfo.year,
                "poster": mediainfo.get_poster_image(),
                "overview": mediainfo.overview,
                "tmdbid": mediainfo.tmdb_id,
                "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
        processed_keys.add(entry_key)

        return {
            "status": "success",
            "status_text": success_text,
            "message": action_message,
        }

    def __build_detail_record(
        self,
        url: str,
        status: str,
        message: str,
        last_time: str,
        trigger_source: str,
        success_total: int,
        skip_total: int,
        error_total: int,
        items: List[dict],
        logs: List[dict],
    ) -> Dict[str, Any]:
        status_text_map = {
            "success": "执行成功",
            "partial": "部分成功",
            "error": "执行失败",
        }
        return {
            "url": url,
            "status": status,
            "status_text": status_text_map.get(status, status),
            "message": message,
            "last_time": last_time,
            "trigger_source": trigger_source,
            "item_total": len(items),
            "success_total": success_total,
            "skip_total": skip_total,
            "error_total": error_total,
            "items": items[:20],
            "logs": logs[-40:],
        }

    def __build_item_info(self, result: dict) -> Dict[str, Any]:
        pubdate = result.get("pubdate")
        pubdate_text = ""
        if pubdate:
            if isinstance(pubdate, datetime.datetime):
                pubdate_text = pubdate.strftime("%Y-%m-%d %H:%M:%S")
            else:
                pubdate_text = str(pubdate)

        return {
            "title": result.get("title") or "未命名条目",
            "link": result.get("link") or result.get("enclosure") or "",
            "description": self.__limit_text(result.get("description") or ""),
            "pubdate": pubdate_text,
            "size_text": self.__format_size(result.get("size") or 0),
            "status": "",
            "status_text": "",
            "message": "",
        }

    def __read_history(self) -> List[dict]:
        history = self.get_data("history") or []
        if not isinstance(history, list):
            return []
        return [item for item in history if isinstance(item, dict)]

    def __read_detail_records(self) -> List[dict]:
        records = self.get_data("rss_detail_records") or []
        if isinstance(records, dict):
            records = list(records.values())
        if not isinstance(records, list):
            return []
        return [item for item in records if isinstance(item, dict)]

    def __get_rss_urls(self) -> List[str]:
        return self.__split_rss_urls(self._address)

    def __prune_removed_urls(self, rss_urls: List[str]):
        active_urls = set(rss_urls)
        history_items = [item for item in self.__read_history() if item.get("url") in active_urls]
        detail_records = [item for item in self.__read_detail_records() if item.get("url") in active_urls]
        self.save_data("history", history_items)
        self.save_data("rss_detail_records", detail_records)

    @staticmethod
    def __split_rss_urls(value: Any) -> List[str]:
        urls: List[str] = []
        seen = set()
        for raw in str(value or "").splitlines():
            url = raw.strip()
            if not url or url in seen:
                continue
            seen.add(url)
            urls.append(url)
        return urls

    @staticmethod
    def __build_history_key(url: str, title: str, enclosure: str, link: str) -> str:
        return f"{url}||{title}||{enclosure or link or ''}"

    @staticmethod
    def __clean_text(value: Any) -> str:
        return str(value or "").strip()

    @staticmethod
    def __format_size(size: Any) -> str:
        try:
            value = float(size)
        except (TypeError, ValueError):
            return ""
        if value <= 0:
            return ""
        units = ["B", "KB", "MB", "GB", "TB"]
        unit_index = 0
        while value >= 1024 and unit_index < len(units) - 1:
            value /= 1024
            unit_index += 1
        return f"{value:.2f} {units[unit_index]}"

    @staticmethod
    def __limit_text(value: str, limit: int = 280) -> str:
        clean_text = re.sub(r"\s+", " ", str(value or "")).strip()
        if len(clean_text) <= limit:
            return clean_text
        return f"{clean_text[:limit]}..."

    def __append_run_log(self, logs: List[dict], level: str, message: str):
        logs.append(
            {
                "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "level": level,
                "message": message,
            }
        )
        if level == "ERROR":
            logger.error(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)

    def __log_and_notify_error(self, message: str):
        logger.error(message)
        self.systemmessage.put(message, title="自定义订阅")

    def __validate_and_fix_config(self, config: dict = None) -> bool:
        size_range = config.get("size_range")
        if size_range and not self.__is_number_or_range(str(size_range)):
            self.__log_and_notify_error(f"自定义订阅出错，种子大小设置错误：{size_range}")
            config["size_range"] = None
            return False
        return True

    @staticmethod
    def __is_number_or_range(value):
        return bool(re.match(r"^\d+(\.\d+)?(-\d+(\.\d+)?)?$", value))

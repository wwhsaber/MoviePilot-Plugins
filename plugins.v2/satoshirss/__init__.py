import datetime
import re
import traceback
from pathlib import Path
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple
from urllib.parse import urlparse

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
    plugin_version = "2.3"
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
        saved_input_url = ""
        if config:
            self.__validate_and_fix_config(config=config)

            rss_urls = self.__split_rss_urls(config.get("address"))
            current_input = self.__clean_text(config.get("rss_url"))
            if current_input:
                need_save = True
                saved_input_url = current_input
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
            if saved_input_url:
                self.__refresh_urls_safely([saved_input_url], "save")

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
                "path": "/delete_history_item",
                "endpoint": self.delete_history_item,
                "methods": ["GET"],
                "summary": "删除作品历史记录",
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
        records = self.__build_source_records()
        return [
            {
                "component": "VForm",
                "props": {
                    "style": "max-height: 70vh; overflow-y: auto; padding-right: 8px;"
                },
                "content": [
                    self.__build_form_section(
                        "1. RSS清单",
                        [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 10},
                                        "content": [
                                            {
                                                "component": "VTextarea",
                                                "props": {
                                                    "model": "address",
                                                    "label": "RSS源清单",
                                                    "rows": 4,
                                                    "placeholder": "一行一个RSS地址，可直接编辑后保存",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 2},
                                        "content": [
                                            {
                                                "component": "VBtn",
                                                "props": {
                                                    "block": True,
                                                    "color": "primary",
                                                    "variant": "elevated",
                                                },
                                                "text": "获取详情",
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
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "text": "直接在这里维护RSS地址，一行一个。保存后会自动抓取新加的RSS；“获取详情”用于刷新当前清单里的全部RSS状态和日志。",
                                },
                            },
                        ],
                    ),
                    self.__build_form_section(
                        "2. 已配置RSS源",
                        self.__build_source_section_content(records),
                    ),
                    self.__build_form_section(
                        "3. 下载设置",
                        [
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
                                                    "model": "save_path",
                                                    "label": "保存目录",
                                                    "placeholder": "/downloads/rss",
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
                                                    "model": "include",
                                                    "label": "包含关键词",
                                                    "placeholder": "多个关键词可用逗号或正则表达式",
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
                                                    "label": "种子大小限制(GB)",
                                                    "placeholder": "如：10 或 3-10",
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
                                                    "label": "排除关键词",
                                                    "placeholder": "多个关键词可用逗号或正则表达式",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                        ],
                    ),
                    self.__build_form_section(
                        "4. 运行设置",
                        [
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
                    ),
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
        historys = self.__read_history()
        if not historys:
            return [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {"class": "text-center"},
                }
            ]

        historys = sorted(historys, key=lambda item: item.get("time") or "", reverse=True)
        contents = []
        for history in historys:
            title = history.get("title")
            poster = history.get("poster")
            mtype = history.get("type")
            time_str = history.get("time")
            history_key = history.get("key")
            contents.append(
                {
                    "component": "VCard",
                    "content": [
                        {
                            "component": "VDialogCloseBtn",
                            "props": {"innerClass": "absolute top-0 right-0"},
                            "events": {
                                "click": {
                                    "api": "plugin/SatoshiRss/delete_history_item",
                                    "method": "get",
                                    "params": {
                                        "key": history_key,
                                        "apikey": settings.API_TOKEN,
                                    },
                                }
                            },
                        },
                        {
                            "component": "div",
                            "props": {
                                "class": "d-flex justify-space-start flex-nowrap flex-row",
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "content": [
                                        {
                                            "component": "VImg",
                                            "props": {
                                                "src": poster,
                                                "height": 120,
                                                "width": 80,
                                                "aspect-ratio": "2/3",
                                                "class": "object-cover shadow ring-gray-500",
                                                "cover": True,
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "div",
                                    "content": [
                                        {
                                            "component": "VCardTitle",
                                            "props": {
                                                "class": "pa-1 pe-5 break-words whitespace-break-spaces"
                                            },
                                            "text": title,
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {"class": "pa-0 px-2"},
                                            "text": f"类型：{mtype}",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {"class": "pa-0 px-2"},
                                            "text": f"时间：{time_str}",
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                }
            )

        return [
            {
                "component": "div",
                "props": {
                    "class": "grid gap-3 grid-info-card",
                },
                "content": contents,
            }
        ]

    @staticmethod
    def __build_form_section(title: str, content: List[dict]) -> dict:
        return {
            "component": "VCard",
            "props": {"variant": "outlined", "class": "mb-4"},
            "content": [
                {
                    "component": "VCardTitle",
                    "text": title,
                },
                {
                    "component": "div",
                    "props": {"class": "px-4 pb-4"},
                    "content": content,
                },
            ],
        }

    def __build_source_section_content(self, records: List[dict]) -> List[dict]:
        if not records:
            return [
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "还没有已保存的RSS源。先在上面的RSS清单里填好地址并保存。",
                    },
                }
            ]

        return [
            {
                "component": "VExpansionPanels",
                "props": {"multiple": True},
                "content": [self.__build_source_panel(record) for record in records],
            }
        ]

    def __build_source_panel(self, record: dict) -> dict:
        status_text, status_color = self.__status_display(record.get("status"))
        logs = (record.get("logs") or [])[-5:]
        log_rows = []
        if logs:
            for log_item in logs:
                log_rows.append(
                    {
                        "component": "div",
                        "props": {"class": "mb-2"},
                        "text": f"[{log_item.get('time')}] {log_item.get('message')}",
                    }
                )
        else:
            log_rows.append(
                {
                    "component": "div",
                    "text": "暂无日志",
                }
            )

        poster_block = []
        if record.get("poster"):
            poster_block = [
                {
                    "component": "VImg",
                    "props": {
                        "src": record.get("poster"),
                        "height": 120,
                        "width": 80,
                        "aspect-ratio": "2/3",
                        "class": "object-cover shadow ring-gray-500 mb-2",
                        "cover": True,
                    },
                }
            ]

        return {
            "component": "VExpansionPanel",
            "content": [
                {
                    "component": "VExpansionPanelTitle",
                    "content": [
                        {
                            "component": "div",
                            "props": {
                                "class": "d-flex align-center justify-space-between w-100 pe-4"
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {"class": "text-subtitle-1 font-weight-medium"},
                                            "text": record.get("display_title") or record.get("source_name") or record.get("url") or "未命名RSS",
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "text-caption"},
                                            "text": record.get("source_name") or record.get("url") or "",
                                        },
                                    ],
                                },
                                {
                                    "component": "VChip",
                                    "props": {
                                        "color": status_color,
                                        "size": "small",
                                        "variant": "tonal",
                                    },
                                    "text": status_text,
                                },
                            ],
                        }
                    ],
                },
                {
                    "component": "VExpansionPanelText",
                    "content": [
                        {
                            "component": "VRow",
                            "content": [
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 4},
                                    "content": [
                                        *poster_block,
                                        {
                                            "component": "div",
                                            "props": {"class": "mb-2"},
                                            "text": f"RSS链接：{record.get('url') or '-'}",
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "mb-2"},
                                            "text": f"最后更新时间：{record.get('last_time') or '-'}",
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "mb-2"},
                                            "text": f"统计：成功 {record.get('success_total', 0)} 个 / 跳过 {record.get('skip_total', 0)} 个 / 失败 {record.get('error_total', 0)} 个",
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "mb-2"},
                                            "text": f"最近作品：{record.get('display_title') or '-'}",
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "mb-2"},
                                            "text": f"结果：{record.get('message') or '-'}",
                                        },
                                        {
                                            "component": "div",
                                            "props": {"class": "d-flex ga-2 flex-wrap"},
                                            "content": [
                                                {
                                                    "component": "VBtn",
                                                    "props": {
                                                        "href": record.get("url") or "",
                                                        "target": "_blank",
                                                        "variant": "outlined",
                                                        "size": "small",
                                                    },
                                                    "text": "打开RSS",
                                                },
                                                {
                                                    "component": "VBtn",
                                                    "props": {
                                                        "color": "error",
                                                        "variant": "outlined",
                                                        "size": "small",
                                                    },
                                                    "text": "删除",
                                                    "events": {
                                                        "click": {
                                                            "api": "plugin/SatoshiRss/delete_history",
                                                            "method": "get",
                                                            "params": {
                                                                "key": record.get("url"),
                                                                "apikey": settings.API_TOKEN,
                                                            },
                                                        }
                                                    },
                                                },
                                            ],
                                        },
                                    ],
                                },
                                {
                                    "component": "VCol",
                                    "props": {"cols": 12, "md": 8},
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {"class": "font-weight-medium mb-2"},
                                            "text": "最近日志：",
                                        },
                                        *log_rows,
                                    ],
                                },
                            ],
                        }
                    ],
                },
            ],
        }

    def __build_source_records(self) -> List[dict]:
        detail_map = {
            item.get("url"): item
            for item in self.__read_detail_records()
            if item.get("url")
        }
        latest_history_map = self.__latest_history_by_url()
        records = []
        for url in self.__get_rss_urls():
            record = dict(detail_map.get(url) or {})
            latest_history = latest_history_map.get(url) or {}
            record["url"] = url
            record["source_name"] = self.__source_name(url)
            record["display_title"] = latest_history.get("title") or record.get("display_title") or record["source_name"]
            record["poster"] = latest_history.get("poster") or record.get("poster") or ""
            if not record.get("status"):
                record["status"] = "idle"
                record["status_text"] = "未获取"
                record["message"] = "保存后会自动抓取一次，或点击“获取详情”刷新"
                record["last_time"] = record.get("last_time") or "-"
                record["success_total"] = record.get("success_total", 0)
                record["skip_total"] = record.get("skip_total", 0)
                record["error_total"] = record.get("error_total", 0)
                record["logs"] = record.get("logs") or []
            records.append(record)
        return records

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

        summary = self.__refresh_urls_safely(urls=urls, trigger_source="manual")
        if not summary:
            return schemas.Response(success=False, message="任务仍在执行中，请稍后再试")

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

    def delete_history_item(self, key: str, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")

        history_items = self.__read_history()
        if not history_items:
            return schemas.Response(success=False, message="未找到历史记录")

        history_items = [item for item in history_items if item.get("key") != key]
        self.save_data("history", history_items)
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

        if not self.__refresh_urls_safely(urls=urls, trigger_source="schedule"):
            logger.info("自定义订阅任务仍在执行中，跳过本次运行")

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

    def __refresh_urls_safely(self, urls: List[str], trigger_source: str) -> Optional[Dict[str, Any]]:
        if not urls:
            return None
        if not lock.acquire(blocking=False):
            return None
        try:
            return self.__run_for_urls(urls=urls, trigger_source=trigger_source)
        finally:
            lock.release()

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
            self.__append_run_log(logs, "ERROR", f"{title} 未识别到有效数据")
            return {
                "status": "error",
                "status_text": "识别失败",
                "message": "未识别到有效媒体信息",
            }

        mediainfo: MediaInfo = self.chain.recognize_media(meta=meta)
        if not mediainfo:
            self.__append_run_log(logs, "ERROR", f"{title} 未识别到媒体信息")
            return {
                "media_title": meta.name or title,
                "media_poster": "",
                "status": "error",
                "status_text": "识别失败",
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

        action_message = ""
        if self._action == "download":
            exist_flag, no_exists = downloadchain.get_no_exists_info(meta=meta, mediainfo=mediainfo)
            if exist_flag:
                self.__append_run_log(logs, "INFO", f"{mediainfo.title_year} 媒体库中已存在")
                return {
                    "media_title": self.__media_display_title(mediainfo, meta),
                    "media_poster": mediainfo.get_poster_image(),
                    "status": "skip",
                    "status_text": "已跳过",
                    "message": "媒体库中已存在",
                }

            if mediainfo.type == MediaType.TV and no_exists:
                season_group = no_exists.get(mediainfo.tmdb_id) if mediainfo.tmdb_id else None
                if season_group:
                    season_info = season_group.get(meta.begin_season or 1)
                    if not season_info:
                        self.__append_run_log(logs, "INFO", f"{mediainfo.title_year} {meta.season} 已存在")
                        return {
                            "media_title": self.__media_display_title(mediainfo, meta),
                            "media_poster": mediainfo.get_poster_image(),
                            "status": "skip",
                            "status_text": "已跳过",
                            "message": "该季已存在",
                        }
                    if season_info.episodes and not set(meta.episode_list).issubset(set(season_info.episodes)):
                        self.__append_run_log(
                            logs,
                            "INFO",
                            f"{mediainfo.title_year} {meta.season_episode} 已存在",
                        )
                        return {
                            "media_title": self.__media_display_title(mediainfo, meta),
                            "media_poster": mediainfo.get_poster_image(),
                            "status": "skip",
                            "status_text": "已跳过",
                            "message": "剧集已存在",
                        }

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
                    "media_title": self.__media_display_title(mediainfo, meta),
                    "media_poster": mediainfo.get_poster_image(),
                    "status": "error",
                    "status_text": "下载失败",
                    "message": action_message,
                }
            action_message = f"下载成功，任务ID：{download_hash}"
            self.__append_run_log(logs, "INFO", f"{title} 下载成功")
            success_text = "下载成功"
        else:
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
                                "media_title": self.__media_display_title(mediainfo, meta),
                                "media_poster": mediainfo.get_poster_image(),
                                "status": "skip",
                                "status_text": "已跳过",
                                "message": "媒体库里已存在",
                            }
            elif exist_info:
                self.__append_run_log(logs, "INFO", f"{mediainfo.title_year} 已存在")
                return {
                    "media_title": self.__media_display_title(mediainfo, meta),
                    "media_poster": mediainfo.get_poster_image(),
                    "status": "skip",
                    "status_text": "已跳过",
                    "message": "媒体库里已存在",
                }

            if subscribechain.exists(mediainfo=mediainfo, meta=meta):
                self.__append_run_log(logs, "INFO", f"{mediainfo.title_year} 正在订阅中")
                return {
                    "media_title": self.__media_display_title(mediainfo, meta),
                    "media_poster": mediainfo.get_poster_image(),
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
            "media_title": self.__media_display_title(mediainfo, meta),
            "media_poster": mediainfo.get_poster_image(),
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
            "display_title": self.__pick_record_title(items),
            "poster": self.__pick_record_poster(items),
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
            "media_title": "",
            "media_poster": "",
            "status": "",
            "status_text": "",
            "message": "",
        }

    def __read_history(self) -> List[dict]:
        history = self.get_data("history") or []
        if not isinstance(history, list):
            return []
        return [item for item in history if isinstance(item, dict)]

    def __latest_history_by_url(self) -> Dict[str, dict]:
        latest_map: Dict[str, dict] = {}
        for item in self.__read_history():
            url = item.get("url")
            if not url:
                continue
            current = latest_map.get(url)
            if not current or (item.get("time") or "") > (current.get("time") or ""):
                latest_map[url] = item
        return latest_map

    def __read_detail_records(self) -> List[dict]:
        records = self.get_data("rss_detail_records") or []
        if isinstance(records, dict):
            records = list(records.values())
        if not isinstance(records, list):
            return []
        return [item for item in records if isinstance(item, dict)]

    def __get_rss_urls(self) -> List[str]:
        return self.__split_rss_urls(self._address)

    @staticmethod
    def __status_display(status: str) -> Tuple[str, str]:
        if status == "success":
            return "成功", "success"
        if status == "partial":
            return "部分成功", "warning"
        if status == "error":
            return "失败", "error"
        return "未获取", "default"

    @staticmethod
    def __source_name(url: str) -> str:
        parsed = urlparse(url or "")
        if parsed.netloc:
            return parsed.netloc
        return url or "未命名RSS"

    @staticmethod
    def __media_display_title(mediainfo: MediaInfo, meta: MetaInfo) -> str:
        season_text = meta.season or ""
        return f"{mediainfo.title} {season_text}".strip()

    @staticmethod
    def __pick_record_title(items: List[dict]) -> str:
        for item in items:
            media_title = item.get("media_title")
            if media_title:
                return media_title
        for item in items:
            title = item.get("title")
            if title:
                return title
        return ""

    @staticmethod
    def __pick_record_poster(items: List[dict]) -> str:
        for item in items:
            media_poster = item.get("media_poster")
            if media_poster:
                return media_poster
        return ""

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

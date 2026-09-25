import datetime
import re
import traceback
import unicodedata
from pathlib import Path
from threading import Lock
from typing import Optional, Any, List, Dict, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.download import DownloadChain
from app.chain.subscribe import SubscribeChain
from app.sdk.config import settings
from app.sdk.media import MediaInfo, TorrentInfo, Context
from app.sdk.media import MetaInfo
from app.sdk.network import RssHelper
from app.sdk.logging import logger
from app.plugins import _PluginBase
from app.schemas import ExistMediaInfo
from app.schemas.types import MediaSource, SystemConfigKey, MediaType
from app.sdk.media import normalize_media_source, resolve_media_identity

lock = Lock()


class RssSubscribeSeasonFix(_PluginBase):
    """改进动漫 RSS 标题及季集号识别的自定义订阅插件。"""

    # 插件名称
    plugin_name = "自定义订阅（季号修复版）"
    # 插件描述
    plugin_desc = "定时刷新 RSS；改进番剧标题解析，默认识别失败后回退匹配媒体库缓存。"
    # 插件图标
    plugin_icon = "customsubscribe.webp"
    # 插件版本
    plugin_version = "1.0.2"
    # 插件作者
    plugin_author = "wwhsaber"
    # 作者主页
    author_url = "https://github.com/wwhsaber"
    # 插件配置项ID前缀
    plugin_config_prefix = "rsssubscribeseasonfix_"
    # 加载顺序
    plugin_order = 19
    # 可使用的用户级别
    auth_level = 2

    # 私有变量
    _scheduler: Optional[BackgroundScheduler] = None
    _cache_path: Optional[Path] = None

    # 配置属性
    _enabled: bool = False
    _cron: str = ""
    _notify: bool = False
    _onlyonce: bool = False
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

        # 停止现有任务
        self.stop_service()

        # 配置
        if config:
            self.__validate_and_fix_config(config=config)
            self._enabled = config.get("enabled")
            self._cron = config.get("cron")
            self._notify = config.get("notify")
            self._onlyonce = config.get("onlyonce")
            self._address = config.get("address")
            self._include = config.get("include")
            self._exclude = config.get("exclude")
            self._proxy = config.get("proxy")
            self._filter = config.get("filter")
            self._clear = config.get("clear")
            self._action = config.get("action")
            self._save_path = config.get("save_path")
            self._size_range = config.get("size_range")

        self.__migrate_history_identity()

        if self._onlyonce:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            logger.info(f"自定义订阅服务启动，立即运行一次")
            self._scheduler.add_job(func=self.check, trigger='date',
                                    run_date=datetime.datetime.now(
                                        tz=pytz.timezone(settings.TZ)) + datetime.timedelta(seconds=3)
                                    )

            # 启动任务
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

        if self._onlyonce or self._clear:
            # 关闭一次性开关
            self._onlyonce = False
            # 记录清理缓存设置
            self._clearflag = self._clear
            # 关闭清理缓存开关
            self._clear = False
            # 保存设置
            self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    def __migrate_history_identity(self) -> None:
        """将旧 RSS 历史的 TMDB ID 原地迁移为统一媒体身份。"""
        history = self.get_data("history")
        if not isinstance(history, list):
            return
        changed = False
        for item in history:
            if not isinstance(item, dict):
                continue
            media_source, media_id = resolve_media_identity(
                media_source=item.get("media_source"),
                media_id=item.get("media_id"),
            )
            if not media_source:
                media_source, media_id = resolve_media_identity(
                    media_source=MediaSource.TMDB,
                    media_id=item.get("tmdbid"),
                )
            if not media_source:
                continue
            migrated = {key: value for key, value in item.items() if key != "tmdbid"}
            migrated["media_source"] = media_source.value
            migrated["media_id"] = media_id
            if migrated != item:
                item.clear()
                item.update(migrated)
                changed = True
        if changed:
            self.save_data("history", history)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """
        定义远程控制命令
        :return: 命令关键字、事件、描述、附带数据
        """
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        """
        获取插件API
        [{
            "path": "/xx",
            "endpoint": self.xxx,
            "methods": ["GET", "POST"],
            "summary": "API说明"
        }]
        """
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": "删除自定义订阅历史记录"
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """
        注册插件公共服务
        [{
            "id": "服务ID",
            "name": "服务名称",
            "trigger": "触发器：cron/interval/date/CronTrigger.from_crontab()",
            "func": self.xxx,
            "kwargs": {} # 定时器参数
        }]
        """
        if self._enabled and self._cron:
            return [{
                "id": self.__class__.__name__,
                "name": "自定义订阅（季号修复版）服务",
                "trigger": CronTrigger.from_crontab(self._cron),
                "func": self.check,
                "kwargs": {}
            }]
        elif self._enabled:
            return [{
                "id": self.__class__.__name__,
                "name": "自定义订阅（季号修复版）服务",
                "trigger": "interval",
                "func": self.check,
                "kwargs": {"minutes": 30}
            }]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """
        拼装插件配置页面，需要返回两块数据：1、页面配置；2、数据结构
        """
        return [
            {
                'component': 'VForm',
                'content': [
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'enabled',
                                            'label': '启用插件',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'notify',
                                            'label': '发送通知',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'onlyonce',
                                            'label': '立即运行一次',
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VCronField',
                                        'props': {
                                            'model': 'cron',
                                            'label': '执行周期',
                                            'placeholder': '5位cron表达式，留空自动'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VSelect',
                                        'props': {
                                            'model': 'action',
                                            'label': '动作',
                                            'items': [
                                                {'title': '订阅', 'value': 'subscribe'},
                                                {'title': '下载', 'value': 'download'}
                                            ]
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12
                                },
                                'content': [
                                    {
                                        'component': 'VTextarea',
                                        'props': {
                                            'model': 'address',
                                            'label': 'RSS地址',
                                            'rows': 3,
                                            'placeholder': '每行一个RSS地址'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'include',
                                            'label': '包含',
                                            'placeholder': '支持正则表达式'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'exclude',
                                            'label': '排除',
                                            'placeholder': '支持正则表达式'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'size_range',
                                            'label': '种子大小(GB)',
                                            'placeholder': '如：3 或 3-5'
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 6
                                },
                                'content': [
                                    {
                                        'component': 'VTextField',
                                        'props': {
                                            'model': 'save_path',
                                            'label': '保存目录',
                                            'placeholder': '下载时有效，留空自动'
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        'component': 'VRow',
                        'content': [
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'proxy',
                                            'label': '使用代理服务器',
                                        }
                                    }
                                ]
                            }, {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4,
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'filter',
                                            'label': '使用订阅优先级规则',
                                        }
                                    }
                                ]
                            },
                            {
                                'component': 'VCol',
                                'props': {
                                    'cols': 12,
                                    'md': 4
                                },
                                'content': [
                                    {
                                        'component': 'VSwitch',
                                        'props': {
                                            'model': 'clear',
                                            'label': '清理历史记录',
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ], {
            "enabled": False,
            "notify": True,
            "onlyonce": False,
            "cron": "*/30 * * * *",
            "address": "",
            "include": "",
            "exclude": "",
            "proxy": False,
            "clear": False,
            "filter": False,
            "action": "subscribe",
            "save_path": "",
            "size_range": ""
        }

    def get_page(self) -> List[dict]:
        """
        拼装插件详情页面，需要返回页面配置，同时附带数据
        """
        # 查询同步详情
        historys = self.get_data('history')
        if not historys:
            return [
                {
                    'component': 'div',
                    'text': '暂无数据',
                    'props': {
                        'class': 'text-center',
                    }
                }
            ]
        # 数据按时间降序排序
        historys = sorted(historys, key=lambda x: x.get('time'), reverse=True)
        # 拼装页面
        contents = []
        for history in historys:
            title = history.get("title")
            poster = history.get("poster")
            mtype = history.get("type")
            time_str = history.get("time")
            contents.append(
                {
                    'component': 'VCard',
                    'content': [
                        {
                            "component": "VDialogCloseBtn",
                            "props": {
                                'innerClass': 'absolute top-0 right-0',
                            },
                            'events': {
                                'click': {
                                    'api': f'plugin/{self.__class__.__name__}/delete_history',
                                    'method': 'get',
                                    'params': {
                                        'key': title,
                                        'apikey': settings.API_TOKEN
                                    }
                                }
                            },
                        },
                        {
                            'component': 'div',
                            'props': {
                                'class': 'd-flex justify-space-start flex-nowrap flex-row',
                            },
                            'content': [
                                {
                                    'component': 'div',
                                    'content': [
                                        {
                                            'component': 'VImg',
                                            'props': {
                                                'src': poster,
                                                'height': 120,
                                                'width': 80,
                                                'aspect-ratio': '2/3',
                                                'class': 'object-cover shadow ring-gray-500',
                                                'cover': True
                                            }
                                        }
                                    ]
                                },
                                {
                                    'component': 'div',
                                    'content': [
                                        {
                                            'component': 'VCardTitle',
                                            'props': {
                                                'class': 'pa-1 pe-5 break-words whitespace-break-spaces'
                                            },
                                            'text': title
                                        },
                                        {
                                            'component': 'VCardText',
                                            'props': {
                                                'class': 'pa-0 px-2'
                                            },
                                            'text': f'类型：{mtype}'
                                        },
                                        {
                                            'component': 'VCardText',
                                            'props': {
                                                'class': 'pa-0 px-2'
                                            },
                                            'text': f'时间：{time_str}'
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            )

        return [
            {
                'component': 'div',
                'props': {
                    'class': 'grid gap-3 grid-info-card',
                },
                'content': contents
            }
        ]

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error("退出插件失败：%s" % str(e))

    def delete_history(self, key: str, apikey: str):
        """
        删除同步历史记录
        """
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")
        # 历史记录
        historys = self.get_data('history')
        if not historys:
            return schemas.Response(success=False, message="未找到历史记录")
        # 删除指定记录
        historys = [h for h in historys if h.get("title") != key]
        self.save_data('history', historys)
        return schemas.Response(success=True, message="删除成功")

    def __update_config(self):
        """
        更新设置
        """
        self.update_config({
            "enabled": self._enabled,
            "notify": self._notify,
            "onlyonce": self._onlyonce,
            "cron": self._cron,
            "address": self._address,
            "include": self._include,
            "exclude": self._exclude,
            "proxy": self._proxy,
            "clear": self._clear,
            "filter": self._filter,
            "action": self._action,
            "save_path": self._save_path,
            "size_range": self._size_range
        })

    @staticmethod
    def __normalize_season_episode_marker(title: str) -> str:
        """将番剧发布名中常见的“第 2 季 - 第 12 集”显式标记成 S02E12。"""
        # 仅接受季号与集号之间明确带空格的形式，避免把“86 - 1”一类标题数字误判成季集号。
        pattern = re.compile(
            r"(?<![A-Za-z0-9])(?P<season>2[0-9]|1[0-9]|[2-9])\s+-\s+"
            r"(?P<episode>\d{1,3})(?P<half>\.5)?(?!\d)(?=\s|$|[\[\]【(（])"
        )
        match = pattern.search(title)
        if not match:
            return title
        season = int(match.group("season"))
        episode = f"{int(match.group('episode')):02d}" + (".5" if match.group("half") else "")
        marker = f"S{season:02d}E{episode}"
        return f"{title[:match.start()]}{marker}{title[match.end():]}"

    @staticmethod
    def __prefer_chinese_title(title: str) -> str:
        """星号发布格式同时带中文名和罗马字名时，优先保留完整中文主标题。"""
        if not re.search(r"[\u3400-\u9fff\u3040-\u30ff]", title):
            return title
        # 仅在标题末尾出现至少两个连续拉丁词时裁掉罗马字别名；短技术词和 S 级等
        # 中英混排词不会命中，避免把真正的中文标题部分截断。
        match = re.search(
            r"\s+(?=[A-Za-z][A-Za-z0-9'’.-]*(?:\s+[A-Za-z][A-Za-z0-9'’.-]*)+$)",
            title,
        )
        if not match or not re.search(r"[\u3400-\u9fff\u3040-\u30ff]", title[:match.start()]):
            return title
        return title[:match.start()].rstrip(" ,，/・-")

    @classmethod
    def __normalize_meta_title(cls, title: str) -> str:
        """按番剧发布名习惯拆出字幕组、标题和季集号，仅供媒体识别使用。"""
        if not title:
            return title

        normalized = unicodedata.normalize("NFKC", title).strip()
        parts = normalized.split("★")
        if len(parts) >= 3 and re.search(r"字幕(?:组|組|社|站)", parts[0], re.IGNORECASE):
            # ANI-RSS 同样显式处理“字幕组★标题★集数★”格式；把纯数字、完结标记或
            # v2 尾缀集号转成 MoviePilot 稳定识别的 S01E01，同时保留后续资源信息。
            episode_match = re.fullmatch(
                r"\s*(\d{1,3})(\.5)?(?:\s*[（(][^）)]*[）)])?(?:\s*[vV]\d+)?\s*",
                parts[2],
            )
            if episode_match and parts[1].strip():
                episode = f"{int(episode_match.group(1)):02d}" + (".5" if episode_match.group(2) else "")
                suffix = " ".join(part.strip() for part in parts[3:] if part.strip())
                series_title = cls.__prefer_chinese_title(parts[1].strip())
                normalized = f"{series_title} S01E{episode} {suffix}".strip()
            else:
                parts = parts[1:]
                normalized = " ".join(part.strip() for part in parts if part.strip())
        elif "★" in normalized:
            # 对没有标准纯数字集号的星号标题仍按分隔符拆开，避免它与标题粘连。
            normalized = " ".join(part.strip() for part in parts if part.strip())

        # 去掉明确标注发布组的前缀，例如 [Nix-Raws]；不触碰普通作品名方括号。
        normalized = re.sub(
            r"^\s*\[(?=[^\]]{1,60}(?:raws|subs|fansub|字幕(?:组|組)|\brip\b))[^\]]{1,60}\]\s*",
            "",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = cls.__normalize_season_episode_marker(normalized)
        return re.sub(r"\s+", " ", normalized).strip()

    @staticmethod
    def __normalize_match_name(name: Optional[str]) -> str:
        """压掉标题标点和空格，便于比较媒体库缓存里的多语言标题。"""
        if not name:
            return ""
        return re.sub(r"[\W_]+", "", unicodedata.normalize("NFKC", str(name)).casefold())

    @classmethod
    def __cache_title_matches(cls, query: Optional[str], candidate: Optional[str]) -> bool:
        """接受完整匹配或至少 8 个字符的连续前缀匹配。"""
        query_name = cls.__normalize_match_name(query)
        candidate_name = cls.__normalize_match_name(candidate)
        if not query_name or not candidate_name:
            return False
        if query_name == candidate_name:
            return True
        shorter, longer = sorted((query_name, candidate_name), key=len)
        return len(shorter) >= 8 and longer.startswith(shorter)

    @staticmethod
    def __identity_key(media_source: Any, media_id: Any) -> Optional[Tuple[str, str]]:
        """把媒体来源和原生 ID 规范成可比较的缓存键。"""
        if not media_source or not media_id:
            return None
        try:
            normalized_source, normalized_id = resolve_media_identity(
                media_source=media_source,
                media_id=media_id,
            )
        except Exception:
            return None
        if not normalized_source or not normalized_id:
            return None
        source_value = getattr(normalized_source, "value", str(normalized_source))
        return str(source_value), str(normalized_id)

    def __load_media_library_cache(self) -> List[Dict[str, Any]]:
        """只读加载 MoviePilot 已同步的媒体库标题与统一媒体身份。"""
        try:
            from app.db.models.mediaserver import MediaServerItem
            from app.db.session import get_session_factory

            with get_session_factory()() as session:
                rows = session.query(
                    MediaServerItem.title,
                    MediaServerItem.original_title,
                    MediaServerItem.year,
                    MediaServerItem.item_type,
                    MediaServerItem.media_source,
                    MediaServerItem.media_id,
                ).filter(
                    MediaServerItem.item_type.in_([MediaType.TV.value, "TV"])
                ).all()
            return [
                {
                    "title": row.title,
                    "original_title": row.original_title,
                    "year": row.year,
                    "item_type": row.item_type,
                    "media_source": row.media_source,
                    "media_id": row.media_id,
                }
                for row in rows
            ]
        except Exception as err:
            logger.warn(f"读取媒体库本地缓存失败：{err}")
            return []

    def __recognize_from_media_library_cache(
        self, meta: MetaInfo, cache_rows: List[Dict[str, Any]]
    ) -> Optional[MediaInfo]:
        """默认识别失败后，使用本地媒体库标题和身份做唯一候选回退。"""
        if not cache_rows or meta.type not in (MediaType.TV, MediaType.TV.value):
            return None

        query_names = [meta.name, meta.cn_name, meta.en_name]
        cached_identities: Dict[Tuple[str, str], Dict[str, Any]] = {}
        title_matches: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for row in cache_rows:
            identity = self.__identity_key(row.get("media_source"), row.get("media_id"))
            if not identity:
                continue
            cached_identities.setdefault(identity, row)
            if any(
                self.__cache_title_matches(query, cached_title)
                for query in query_names
                for cached_title in (row.get("title"), row.get("original_title"))
            ):
                title_matches.setdefault(identity, row)

        def recognize_identity(identity: Tuple[str, str], row: Dict[str, Any]) -> Optional[MediaInfo]:
            media_source = normalize_media_source(identity[0])
            if not media_source:
                return None
            logger.info(
                f"{meta.name} 默认识别未命中，媒体库缓存标题匹配到：{row.get('title')} "
                f"({identity[0]}:{identity[1]})"
            )
            return self.chain.recognize_media(
                meta=meta,
                mtype=MediaType.TV,
                media_source=media_source,
                media_id=identity[1],
            )

        if len(title_matches) == 1:
            identity, row = next(iter(title_matches.items()))
            return recognize_identity(identity, row)
        if len(title_matches) > 1:
            logger.info(f"{meta.name} 在媒体库缓存中命中多个标题，继续尝试别名核对")

        # 有些 RSS 使用 TMDB 的另一条中文译名，而媒体库缓存只保存主标题和原名。
        # 此时先由 TMDB 搜索缩小候选，再要求候选身份已存在于媒体库且详情别名匹配。
        try:
            search_results = self.chain.search_medias(meta=meta, media_source=MediaSource.TMDB) or []
        except Exception as err:
            logger.warn(f"{meta.name} 媒体库缓存别名回退搜索失败：{err}")
            return None

        candidate_identities: List[Tuple[str, str]] = []
        for candidate in search_results:
            if getattr(candidate, "type", None) not in (MediaType.TV, MediaType.TV.value):
                continue
            candidate_identity = self.__identity_key(
                getattr(candidate, "media_source", None),
                getattr(candidate, "media_id", None),
            )
            if not candidate_identity and getattr(candidate, "tmdb_id", None):
                candidate_identity = self.__identity_key(MediaSource.TMDB, candidate.tmdb_id)
            if candidate_identity in cached_identities and candidate_identity not in candidate_identities:
                candidate_identities.append(candidate_identity)
            if len(candidate_identities) >= 20:
                break

        verified: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for identity in candidate_identities:
            if identity[0] != getattr(MediaSource.TMDB, "value", str(MediaSource.TMDB)):
                continue
            try:
                details = self.chain.tmdb_info(tmdbid=int(identity[1]), mtype=MediaType.TV)
            except Exception as err:
                logger.warn(f"读取媒体库候选 TMDB 别名失败（{identity[1]}）：{err}")
                continue
            if not details:
                continue
            detail_names = [
                details.get("name"), details.get("original_name"),
                details.get("title"), details.get("original_title"),
            ]
            for alias in details.get("names") or []:
                if isinstance(alias, str):
                    detail_names.append(alias)
                elif isinstance(alias, dict):
                    detail_names.extend((alias.get("name"), alias.get("title")))
            if any(
                self.__cache_title_matches(query, candidate_name)
                for query in query_names
                for candidate_name in detail_names
            ):
                verified[identity] = cached_identities[identity]

        if len(verified) == 1:
            identity, row = next(iter(verified.items()))
            return recognize_identity(identity, row)
        if len(verified) > 1:
            logger.info(f"{meta.name} 的 TMDB 别名对应多个媒体库条目，放弃自动匹配")
        return None

    def check(self):
        """
        通过用户RSS同步豆瓣想看数据
        """
        if not self._address:
            return
        # 读取历史记录
        if self._clearflag:
            history = []
        else:
            history: List[dict] = self.get_data('history') or []
        downloadchain = DownloadChain()
        subscribechain = SubscribeChain()
        media_cache_rows: Optional[List[Dict[str, Any]]] = None
        for url in self._address.split("\n"):
            # 处理每一个RSS链接
            if not url:
                continue
            logger.info(f"开始刷新RSS：{url} ...")
            results = RssHelper().parse(url, proxy=self._proxy)
            if not results:
                logger.error(f"未获取到RSS数据：{url}")
                return
            # 过滤规则
            filter_groups = self.systemconfig.get(SystemConfigKey.SubscribeFilterRuleGroups)
            # 解析数据
            for result in results:
                try:
                    title = result.get("title")
                    description = result.get("description")
                    enclosure = result.get("enclosure")
                    link = result.get("link")
                    size = result.get("size")
                    pubdate: datetime.datetime = result.get("pubdate")
                    # 检查是否处理过
                    if not title or title in [h.get("key") for h in history]:
                        continue
                    # 检查规则
                    if self._include and not re.search(r"%s" % self._include,
                                                       f"{title} {description}", re.IGNORECASE):
                        logger.info(f"{title} - {description} 不符合包含规则")
                        continue
                    if self._exclude and re.search(r"%s" % self._exclude,
                                                   f"{title} {description}", re.IGNORECASE):
                        logger.info(f"{title} - {description} 不符合排除规则")
                        continue
                    if self._size_range:
                        sizes = [float(_size) * 1024 ** 3 for _size in self._size_range.split("-")]
                        if len(sizes) == 1 and float(size) < sizes[0]:
                            logger.info(f"{title} - 种子大小不符合条件")
                            continue
                        elif len(sizes) > 1 and not sizes[0] <= float(size) <= sizes[1]:
                            logger.info(f"{title} - 种子大小不在指定范围")
                            continue
                    # 识别媒体信息
                    meta_title = self.__normalize_meta_title(title)
                    meta = MetaInfo(title=meta_title, subtitle=description)
                    if not meta.name:
                        logger.warn(f"{title} 未识别到有效数据")
                        continue
                    # 番剧发布名只带集号时，默认按第 1 季处理；“2 - 12”会在标题预处理阶段
                    # 转成 S02E12，因此不会落入此默认分支。
                    if (meta.type in (MediaType.TV, MediaType.TV.value)
                            and meta.begin_episode is not None and meta.begin_season is None):
                        meta.begin_season = 1
                    mediainfo: MediaInfo = self.chain.recognize_media(meta=meta)
                    if not mediainfo:
                        if media_cache_rows is None:
                            media_cache_rows = self.__load_media_library_cache()
                        mediainfo = self.__recognize_from_media_library_cache(meta, media_cache_rows)
                        if not mediainfo:
                            logger.warn(f'未识别到媒体信息，标题：{title}')
                            continue
                    # 种子
                    torrentinfo = TorrentInfo(
                        title=title,
                        description=description,
                        enclosure=enclosure,
                        page_url=link,
                        size=size,
                        pubdate=pubdate.strftime("%Y-%m-%d %H:%M:%S") if pubdate else None,
                        site_proxy=self._proxy,
                    )
                    # 过滤种子
                    if self._filter:
                        result = self.chain.filter_torrents(
                            rule_groups=filter_groups,
                            torrent_list=[torrentinfo],
                            mediainfo=mediainfo
                        )
                        if not result:
                            logger.info(f"{title} {description} 不匹配过滤规则")
                            continue
                    # 媒体库已存在的剧集
                    exist_info: Optional[ExistMediaInfo] = self.chain.media_exists(mediainfo=mediainfo)
                    if mediainfo.type == MediaType.TV:
                        if exist_info:
                            exist_season = exist_info.seasons
                            if exist_season:
                                # 季号未写在发布标题里时，按标准默认值第 1 季检查媒体库。
                                season_number = meta.begin_season
                                if season_number is None:
                                    season_number = 1
                                exist_episodes = exist_season.get(season_number)
                                if exist_episodes and set(meta.episode_list).issubset(set(exist_episodes)):
                                    logger.info(f'{mediainfo.title_year} {meta.season_episode} 己存在')
                                    continue
                    elif exist_info:
                        # 电影已存在
                        logger.info(f'{mediainfo.title_year} 己存在')
                        continue
                    # 下载或订阅
                    if self._action == "download":
                        # 添加下载
                        result = downloadchain.download_single(
                            context=Context(
                                meta_info=meta,
                                media_info=mediainfo,
                                torrent_info=torrentinfo,
                            ),
                            save_path=self._save_path,
                            username="RSS订阅"
                        )
                        if not result:
                            logger.error(f'{title} 下载失败')
                            continue
                    else:
                        # 检查是否在订阅中
                        subflag = subscribechain.exists(mediainfo=mediainfo, meta=meta)
                        if subflag:
                            logger.info(f'{mediainfo.title_year} {meta.season} 正在订阅中')
                            continue
                        # 添加订阅
                        subscribechain.add(title=mediainfo.title,
                                           year=mediainfo.year,
                                           mtype=mediainfo.type,
                                           media_source=mediainfo.media_source,
                                           media_id=mediainfo.media_id,
                                           season=meta.begin_season,
                                           exist_ok=True,
                                           username="RSS订阅")
                    # 存储历史记录
                    history.append({
                        "title": f"{mediainfo.title} {meta.season}",
                        "key": f"{title}",
                        "type": mediainfo.type.value,
                        "year": mediainfo.year,
                        "poster": mediainfo.get_poster_image(),
                        "overview": mediainfo.overview,
                        "media_source": mediainfo.media_source.value,
                        "media_id": str(mediainfo.media_id),
                        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                except Exception as err:
                    logger.error(f'刷新RSS数据出错：{str(err)} - {traceback.format_exc()}')
            logger.info(f"RSS {url} 刷新完成")
        # 保存历史记录
        self.save_data('history', history)
        # 缓存只清理一次
        self._clearflag = False

    def __log_and_notify_error(self, message):
        """
        记录错误日志并发送系统通知
        """
        logger.error(message)
        self.systemmessage.put(message, title="自定义订阅")

    def __validate_and_fix_config(self, config: dict = None) -> bool:
        """
        检查并修正配置值
        """
        size_range = config.get("size_range")
        if size_range and not self.__is_number_or_range(str(size_range)):
            self.__log_and_notify_error(f"自定义订阅出错，种子大小设置错误：{size_range}")
            config["size_range"] = None
            return False
        return True

    @staticmethod
    def __is_number_or_range(value):
        """
        检查字符串是否表示单个数字或数字范围（如'5', '5.5', '5-10' 或 '5.5-10.2'）
        """
        return bool(re.match(r"^\d+(\.\d+)?(-\d+(\.\d+)?)?$", value))

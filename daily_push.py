#!/usr/bin/env python3
"""
每日热梗·生活资讯推送（云端版）
================================
1. 采集多平台热梗 / 冷笑话 / 电影 / 演唱会 / 旅游
2. 渲染为美观的 HTML 页面（输出到 docs/）
3. 推送企业微信图文卡片，点击跳转到 HTML 详情页

环境变量：
  WEBHOOK_URL  企业微信群机器人 Webhook 地址（推荐推送方式）：
                 https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=<KEY>
  BOT_ID       企业微信智能机器人 BotID（WebSocket 长连接，备用）
  BOT_SECRET   智能机器人长连接专用 Secret（备用）
  CHAT_ID      推送目标会话 ID（仅智能机器人模式）：单聊填 userid，群聊填群 chatid
  CHAT_TYPE    1=单聊（默认） / 2=群聊（仅智能机器人模式）
  PAGES_URL    HTML 托管根地址（云端模式必填），如：
                 https://<user>.github.io/<repo>
  MODE         html | push | all（默认 all）
"""

from __future__ import annotations

import base64
import hashlib
import html as html_mod
import json
import logging
import os
import random
import re
import sys
import threading
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import websocket
except ImportError:
    websocket = None


BASE_API = "https://60s.viki.moe/v2"
HTTP_TIMEOUT = 10
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)

# uuhb.cn 每日60秒早报图（PNG 长图，稳定不限流）
UUHB_60S_IMAGE_URL = "https://v1.uuhb.cn/v1/60s/image"
IMAGE_MAX_BYTES = 2 * 1024 * 1024  # 企业微信图片消息上限：2MB

ROOT = Path(__file__).resolve().parent
DOCS_DIR = ROOT / "docs"
LOG_PATH = ROOT / "push.log"

# 企业微信智能机器人（长连接）推送凭证 —— 通过仓库 Secrets 配置，不硬编码
BOT_ID = os.environ.get("BOT_ID") or ""
BOT_SECRET = os.environ.get("BOT_SECRET") or ""
CHAT_ID = os.environ.get("CHAT_ID") or ""            # 单聊填企业微信 userid；群聊填群 chatid
CHAT_TYPE = int(os.environ.get("CHAT_TYPE") or "1")  # 1=单聊（默认）, 2=群聊
PAGES_URL = (os.environ.get("PAGES_URL") or "").rstrip("/")
MODE = os.environ.get("MODE", "all").lower()
# 群机器人 Webhook 地址（推荐推送方式）：https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=<KEY>
WEBHOOK_URL = (os.environ.get("WEBHOOK_URL") or "").strip()

FALLBACK_COVER = (
    "https://images.unsplash.com/photo-1504608524841-42fe6f032b4b"
    "?auto=format&fit=crop&w=1068&h=455&q=80"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("daily_push")


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

def http_get_json(path: str, retries: int = 2) -> dict[str, Any] | None:
    """带指数退避重试的 GET，降低 CI 环境偶发 429/500 导致整段空白。"""
    url = f"{BASE_API}{path}"
    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if payload.get("code") != 200:
                log.warning("API %s code=%s", path, payload.get("code"))
                return None
            return payload.get("data")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_exc = exc
            wait = 2 ** attempt
            log.warning("API %s attempt %d/%d failed: %s", path, attempt + 1, retries + 1, exc)
            if attempt < retries:
                log.info("Retrying %s in %ds...", path, wait)
                time.sleep(wait)
    log.warning("API %s all attempts failed: %s", path, last_exc)
    return None


def download_image(url: str, max_bytes: int = IMAGE_MAX_BYTES) -> bytes | None:
    """下载图片并校验大小与格式；失败返回 None。"""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            data = resp.read()
        if len(data) > max_bytes:
            log.warning("图片超过大小限制: %d bytes (max %d)", len(data), max_bytes)
            return None
        if not data.startswith(b"\x89PNG"):
            log.warning("图片不是 PNG, 前8字节: %s", data[:8])
            return None
        log.info("✅ 图片下载成功: %d bytes", len(data))
        return data
    except (urllib.error.URLError, TimeoutError) as exc:
        log.warning("图片下载失败: %s", exc)
        return None


def image_payload(image_data: bytes) -> dict[str, Any]:
    """生成企业微信图片消息体（base64 + md5），群机器人 Webhook 专用。"""
    b64 = base64.b64encode(image_data).decode("utf-8")
    md5_hash = hashlib.md5(image_data).hexdigest()
    return {"msgtype": "image", "image": {"base64": b64, "md5": md5_hash}}


def news_payload(title: str, description: str, url: str, picurl: str) -> dict[str, Any]:
    """生成企业微信图文消息体，智能机器人单聊/群聊可用（picurl 用外部图片地址）。"""
    return {
        "msgtype": "news",
        "news": {
            "articles": [
                {
                    "title": title,
                    "description": description,
                    "url": url,
                    "picurl": picurl,
                }
            ]
        },
    }


def http_post_json(url: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": USER_AGENT},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ---------------------------------------------------------------------------
# Content collectors
# ---------------------------------------------------------------------------

SERIOUS_KEYWORDS = (
    "死", "亡", "遇难", "坠", "判", "枪", "爆炸", "地震", "海啸",
    "虐", "凶", "命案", "遗骸", "战争", "空袭", "袭击", "重伤",
    "命丧", "灾", "病逝", "疫情", "癌症", "自杀", "被杀", "杀害",
)


def _is_fun(title: str) -> bool:
    return bool(title) and not any(k in title for k in SERIOUS_KEYWORDS)


# ---- 热搜分类关键词 ----------------------------------------------------
# 顺序代表优先级：先匹配上的分类即为该条归属。
HOT_CATEGORIES: list[tuple[str, tuple[str, ...]]] = [
    ("🤖 科技与 AI", (
        "AI", "ai", "大模型", "GPT", "ChatGPT", "OpenAI", "Claude", "Gemini",
        "Sora", "智谱", "DeepSeek", "豆包", "字节", "谷歌", "微软",
        "苹果", "iPhone", "iPad", "Mac", "库克", "华为", "小米", "汽车",
        "特斯拉", "马斯克", "芯片", "机器人", "算力", "量子", "算法",
        "程序员", "代码", "编程", "科技", "科学家", "研究",
    )),
    ("🎭 娱乐与明星", (
        "周杰伦", "林俊杰", "汪苏泷", "许嵩", "王力宏", "陈奕迅", "薛之谦",
        "明星", "演员", "艺人", "歌手", "演唱会", "巡演", "综艺",
        "电视剧", "电影", "剧", "网红", "主播", "直播", "出道", "官宣",
        "CP", "恋情", "结婚", "离婚", "分手", "粉丝", "偶像", "流量",
        "娱乐圈", "红毯", "颁奖", "选秀", "偶像",
    )),
    ("⚽ 体育赛事", (
        "足球", "篮球", "乒乓", "羽毛球", "世界杯", "亚洲杯", "奥运",
        "NBA", "CBA", "欧冠", "英超", "西甲", "德甲", "亚冠", "冠军",
        "夺冠", "决赛", "半决赛", "联赛", "国足", "国乒", "梅西", "C罗",
    )),
    ("🎮 游戏与二次元", (
        "游戏", "原神", "星穹铁道", "崩坏", "王者荣耀", "英雄联盟", "LOL",
        "LPL", "DOTA", "Steam", "Switch", "手游", "主机", "电竞",
        "动漫", "漫画", "二次元", "番剧", "原神", "鸣潮",
    )),
    ("💰 商业与财经", (
        "股", "A 股", "基金", "房价", "楼市", "房产", "经济", "财经",
        "投资", "上市", "退市", "破产", "融资", "IPO", "收购", "并购",
        "CEO", "创始人", "首富", "老板", "员工", "裁员", "加薪", "年终奖",
        "996", "工资", "薪资", "GDP", "消费", "通胀",
    )),
    ("🍜 美食与生活", (
        "美食", "餐厅", "零食", "奶茶", "咖啡", "外卖", "霸王茶姬",
        "瑞幸", "星巴克", "海底捞", "减肥", "健身", "瑜伽", "穿搭",
        "美妆", "护肤", "时尚", "育儿", "教育", "考研", "高考", "中考",
        "就业", "求职", "面试", "上班", "健康", "养生", "睡眠",
    )),
    ("🏛 时事与社会", (
        "政策", "国家", "中央", "部长", "市长", "总理", "总统", "外交",
        "国际", "新规", "法律", "法案", "通胀", "关税", "制裁", "协议",
        "会议", "访问", "签署", "发布", "启动",
    )),
    ("🌏 旅行与地理", (
        "旅游", "旅行", "景区", "樱花", "油菜花", "花海", "日落", "日出",
        "看海", "爬山", "徒步", "露营", "出差", "机票", "酒店", "民宿",
        "攻略", "穷游", "自驾",
    )),
]


def _classify(title: str) -> str:
    for cat, keys in HOT_CATEGORIES:
        if any(k in title for k in keys):
            return cat
    return "💬 其他话题"


# ---- 相似话题去重（Jaccard + 中文 2-gram）--------------------------------
_STOP = set("的了是和与及或在也就都还又把被从向于到为而且但即如果我你他她它们这那个么吧嘛啊吗哦有没又去来上下新新版热门消息曝光官宣回应称上热搜")


def _title_tokens(title: str) -> set[str]:
    """提取标题的指纹词集：中文按 2-gram；英文/数字按整词（≥2）。"""
    # 去除标点/空白/emoji（保留中英数）
    clean = re.sub(r"[^\w\u4e00-\u9fff]+", " ", title)
    toks: set[str] = set()
    for seg in clean.split():
        if not seg:
            continue
        if any("\u4e00" <= c <= "\u9fff" for c in seg):
            # 中文 2-gram
            for i in range(len(seg) - 1):
                g = seg[i:i + 2]
                if g not in _STOP:
                    toks.add(g)
        else:
            w = seg.lower()
            if len(w) >= 2 and w not in _STOP:
                toks.add(w)
    return toks


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / len(a | b)


def _dedupe_similar(
    items: list[dict[str, str]],
    threshold: float = 0.5,
) -> list[dict[str, str]]:
    """同分类内用 Jaccard 相似度去重，先出现的（热度更高）优先保留。"""
    kept: list[dict[str, str]] = []
    kept_tokens: list[set[str]] = []
    for it in items:
        tokens = _title_tokens(it["title"])
        if not tokens:
            continue
        if any(_jaccard(tokens, kt) >= threshold for kt in kept_tokens):
            continue
        kept.append(it)
        kept_tokens.append(tokens)
    return kept


def collect_hot_by_category() -> dict[str, list[dict[str, str]]]:
    """抓取微博/抖音/小红书/B 站热搜各前 20，去重后按分类组织。"""
    sources = [
        ("/weibo", "微博"),
        ("/douyin", "抖音"),
        ("/rednote", "小红书"),
        ("/bili", "B 站"),
    ]
    seen_titles: set[str] = set()
    buckets: dict[str, list[dict[str, str]]] = {}
    # 预先占位，保证分类顺序稳定
    for cat, _ in HOT_CATEGORIES:
        buckets[cat] = []
    buckets["💬 其他话题"] = []

    for path, label in sources:
        data = http_get_json(path)
        if not isinstance(data, list):
            continue
        for item in data[:20]:
            title = (item.get("title") or "").strip()
            if not title or title in seen_titles or not _is_fun(title):
                continue
            # 简易去重：标题完全一样 → 跳过
            seen_titles.add(title)
            cat = _classify(title)
            buckets[cat].append({
                "title": title,
                "source": label,
                "link": item.get("link") or "",
            })

    # 相似话题去重（Jaccard 0.5，同分类内）+ 每类限 6 条
    out: dict[str, list[dict[str, str]]] = {}
    for cat, items in buckets.items():
        if not items:
            continue
        deduped = _dedupe_similar(items, threshold=0.5)
        if deduped:
            out[cat] = deduped[:6]
    return out


def collect_jokes(limit: int = 10) -> list[str]:
    jokes: list[str] = []
    seen: set[str] = set()
    for _ in range(limit * 3):
        data = http_get_json("/duanzi")
        if isinstance(data, dict):
            text = (data.get("duanzi") or "").strip()
            if text and text not in seen:
                seen.add(text)
                jokes.append(text)
        if len(jokes) >= limit:
            break

    fallback = [
        "我闺蜜减肥成功后，前男友想复合。她冷冷地说：当初你嫌我重，现在我轻了，但你更轻。",
        "老板说今年业绩不好，年终奖发不了。我说没事，我年初就没指望。老板愣了一下：那明年你也别指望了。",
        "朋友问我为什么不谈恋爱，我说我对自己要求很高。她点点头：嗯，你这要求确实太高了，一般人达不到。",
        "我妈让我找对象要门当户对，于是我找了个像我一样没对象的。",
        "我跟同事说我精神状态不好，他安慰我：没事，大家精神状态都不好，大家都在硬撑。这话听完我觉得更不好了。",
        "昨天买了一支贵得离谱的口红，今天早上出门戴了口罩，感觉像是花钱买了个心理安慰。",
        "我问 AI：怎样才能更自律？AI 说：建议你关掉手机。我想了想，关掉了 AI。",
        "最近终于攒了点钱，决定奖励自己。然后看了眼余额，决定继续批评自己。",
        "室友今天买了一束花放桌上，我以为他恋爱了。一问才知道是自己送给自己的——他说花会谢，人不一定会来。",
        "早上挤地铁，前面一个人突然回头对我笑。我还没反应过来，他说：兄弟，你踩我脚了，已经两站。",
    ]
    while len(jokes) < limit:
        pick = random.choice(fallback)
        if pick not in seen:
            seen.add(pick)
            jokes.append(pick)
    return jokes[:limit]


# ---- 影音 -------------------------------------------------------------------

def _parse_douban_item(d: dict[str, Any], kind: str) -> dict[str, str]:
    name = (d.get("title") or "").strip()
    subtitle_parts = (d.get("card_subtitle") or "").split("/")
    meta = (
        " / ".join(s.strip() for s in subtitle_parts[2:4])
        if len(subtitle_parts) >= 4 else ""
    )
    return {
        "kind": kind,
        "title": name,
        "subtitle": f"豆瓣 {d.get('rating', '-')}",
        "meta": meta,
        "cover": d.get("cover_proxy") or d.get("cover") or "",
        "link": d.get("url") or f"https://www.douban.com/search?q={urllib.parse.quote(name)}",
    }


def collect_media() -> dict[str, list[dict[str, str]]]:
    """
    返回影音三类：movies / tvs / musics
    - 电影：豆瓣每周榜 top 3
    - 电视剧：国产 + 全球各 2 部
    - 音乐：网易云飙升榜 top 5
    """
    media: dict[str, list[dict[str, str]]] = {"movies": [], "tvs": [], "musics": []}

    movies = http_get_json("/douban/weekly/movie") or []
    if isinstance(movies, list):
        for d in movies[:3]:
            media["movies"].append(_parse_douban_item(d, "🎬 电影"))

    tv_cn = http_get_json("/douban/weekly/tv_chinese") or []
    if isinstance(tv_cn, list):
        for d in tv_cn[:2]:
            media["tvs"].append(_parse_douban_item(d, "📺 国产剧"))

    tv_gl = http_get_json("/douban/weekly/tv_global") or []
    if isinstance(tv_gl, list):
        for d in tv_gl[:2]:
            media["tvs"].append(_parse_douban_item(d, "📺 海外剧"))

    # 网易云飙升榜 (ID 19723756) ——最能代表当下流行趋势
    ncm = http_get_json("/ncm-rank/19723756") or []
    if isinstance(ncm, list):
        for s in ncm[:5]:
            artists = s.get("artist") or []
            artist_names = " / ".join(a.get("name", "") for a in artists if a.get("name"))
            album = (s.get("album") or {}).get("name", "")
            media["musics"].append({
                "kind": "🎵 流行音乐",
                "title": s.get("title") or "",
                "subtitle": f"{artist_names}",
                "meta": f"专辑《{album}》" if album else "",
                "cover": (s.get("album") or {}).get("cover", ""),
                "link": s.get("link") or "",
            })

    return media


def get_daily_news() -> list[dict[str, str]]:
    """
    返回 60 秒每日新闻，每条是 {text, link}
    点击后跳转到百度搜索该新闻关键词。
    """
    data = http_get_json("/60s") or {}
    raw = data.get("news") or []
    out: list[dict[str, str]] = []
    for text in raw:
        # 提取前 20 字作为搜索关键词（去掉开头的数字年份等）
        cleaned = text.split("，", 1)[0].split("：", 1)[0]
        kw = cleaned[:20]
        out.append({
            "text": text,
            "link": f"https://www.baidu.com/s?wd={urllib.parse.quote(kw)}&tn=news",
        })
    return out


# ---------------------------------------------------------------------------
# 演唱会：重点艺人列表（用户偏好）
# ---------------------------------------------------------------------------

FAVORITE_ARTISTS: list[dict[str, str]] = [
    {"name": "汪苏泷", "tag": "情歌诗人", "style": "流行/情歌"},
    {"name": "许嵩",   "tag": "V 仙级创作歌手", "style": "中国风 / 创作"},
    {"name": "徐良",   "tag": "非主流时代经典", "style": "情歌 / 复古"},
    {"name": "王力宏", "tag": "华语 R&B 代表", "style": "R&B / 抒情"},
    {"name": "周杰伦", "tag": "华语天王", "style": "流行 / 经典"},
    {"name": "陈粒",   "tag": "小众清新派", "style": "民谣 / 独立"},
    {"name": "李荣浩", "tag": "全能唱作人", "style": "流行 / 都市"},
    {"name": "林俊杰", "tag": "JJ20 FINAL LAP 尾声", "style": "流行 / 抒情"},
    {"name": "告五人", "tag": "金曲台独立乐团", "style": "独立 / 现场"},
]


def collect_concerts(limit: int = 5) -> list[dict[str, str]]:
    """
    每天随机挑 N 位主要艺人，给出大麦网搜索链接（点击看最新场次）。
    """
    picks = random.sample(FAVORITE_ARTISTS, k=min(limit, len(FAVORITE_ARTISTS)))
    out = []
    for a in picks:
        kw = f"{a['name']} 演唱会"
        out.append({
            "artist": a["name"],
            "tag": a["tag"],
            "style": a["style"],
            "link": f"https://search.damai.cn/search.html?keyword={urllib.parse.quote(kw)}",
        })
    return out


# ---------------------------------------------------------------------------
# 旅游：按月份推荐，每条含目的地、季节理由、深圳出发交通
# 距离深圳 <= 3 小时优先；季节限定强的也会纳入
# ---------------------------------------------------------------------------

TRAVEL_BY_MONTH: dict[int, list[dict[str, str]]] = {
    1: [
        {
            "name": "哈尔滨·冰雪大世界",
            "season": "隆冬冰雕最盛期，零下 20℃ 的极致雪国体验",
            "access": "深圳宝安 → 哈尔滨直飞约 5h；冰雪大世界在市区打车可达",
            "duration": "4-5 天",
        },
        {
            "name": "广东韶关·丹霞山",
            "season": "冬日山体赤红分明，云海雾凇偶尔可遇",
            "access": "深圳北 → 韶关 高铁 1h30；景区打车 30 分钟",
            "duration": "2 天 1 晚",
        },
        {
            "name": "海南三亚·后海",
            "season": "全国最暖冬日海滨，冲浪水温 22℃",
            "access": "深圳宝安 → 三亚凤凰 1h40；打车至后海 1h",
            "duration": "3-4 天",
        },
    ],
    2: [
        {
            "name": "广东云浮·罗浮山 + 温泉",
            "season": "早春山茶花开，山脚温泉最宜泡",
            "access": "深圳北 → 惠州南 高铁 30 分钟，自驾 / 包车入山",
            "duration": "2 天 1 晚",
        },
        {
            "name": "厦门·鼓浪屿",
            "season": "春节南下避寒，气温 15-20℃，海岛文艺氛围",
            "access": "深圳北 → 厦门北 高铁 3h30；或直飞 1h30",
            "duration": "3 天",
        },
        {
            "name": "云南元阳·哈尼梯田",
            "season": "冬末灌水期，日出云海最为壮观",
            "access": "深圳宝安 → 昆明 2h，转高铁至建水 2h，包车入梯田",
            "duration": "4-5 天",
        },
    ],
    3: [
        {
            "name": "江西婺源·油菜花",
            "season": "3 月下旬全国最经典的金色花海盛期",
            "access": "深圳北 → 婺源 高铁 6h；或飞南昌转高铁 3h",
            "duration": "3-4 天",
        },
        {
            "name": "广东惠州·巽寮湾",
            "season": "春海气温回暖，水清沙白适合家庭短途",
            "access": "深圳 → 巽寮湾 自驾 2h；或包车",
            "duration": "2 天 1 晚",
        },
        {
            "name": "福建漳州·东山岛",
            "season": "3 月海风温柔，日落金黄色最上镜",
            "access": "深圳北 → 潮汕站 高铁 1h20 + 自驾 1h30 到东山",
            "duration": "3 天",
        },
    ],
    4: [
        {
            "name": "广西阳朔·漓江 + 遇龙河",
            "season": "4 月春雨过后山水最润，竹筏皮划艇黄金期",
            "access": "深圳北 → 桂林 高铁 3h30；阳朔打车 1h20",
            "duration": "3 天 2 晚",
            "why_sz": "距离深圳最近的春日山水之一，整周末往返可行",
        },
        {
            "name": "江西龙虎山 + 武夷山双程",
            "season": "清明前后春茶季开采，丹霞绿水对比强烈",
            "access": "深圳北 → 武夷山东 高铁 5h30；或直飞武夷山 1h40",
            "duration": "4-5 天",
            "why_sz": "广东出发最方便的春茶行程",
        },
        {
            "name": "湖南张家界·国家森林公园",
            "season": "谷雨后新绿，云雾山景如水墨",
            "access": "深圳宝安 → 张家界荷花 1h50；景区大巴 30 分钟",
            "duration": "4 天",
            "why_sz": "直飞航班多，票价友好",
        },
        {
            "name": "新疆伊犁·杏花沟",
            "season": "4 月中下旬限定·粉色杏花海仅 10 天花期",
            "access": "深圳宝安 → 伊宁 直飞约 6h；包车入沟 1h",
            "duration": "7-8 天",
            "why_sz": "远但值得，需要提前订机票",
        },
        {
            "name": "河南洛阳·牡丹花会",
            "season": "谷雨前后牡丹盛开，千年国花传统",
            "access": "深圳北 → 洛阳龙门 高铁 6h；或飞郑州 2h20 转高铁",
            "duration": "3-4 天",
            "why_sz": "可与龙门石窟、少林寺一起安排",
        },
    ],
    5: [
        {
            "name": "青海湖 + 茶卡盐湖",
            "season": "5 月起天空之镜开放，人少景美",
            "access": "深圳 → 西宁 直飞 4h；包车环湖 3 天",
            "duration": "5-6 天",
        },
        {
            "name": "广东南岭·徒步",
            "season": "五一前后高山杜鹃盛花期",
            "access": "深圳北 → 韶关 高铁 1h30 + 自驾 1h 入山",
            "duration": "2 天 1 晚",
        },
        {
            "name": "四川稻城亚丁",
            "season": "五一草甸返青，雪山清晰度全年最佳",
            "access": "深圳 → 成都 2h40，转机至亚丁机场 1h",
            "duration": "5-7 天",
        },
    ],
    6: [
        {
            "name": "内蒙古·呼伦贝尔大草原",
            "season": "6 月草原返青，夜晚可睡星空房",
            "access": "深圳 → 海拉尔 经停 1 次 6h；包车环线",
            "duration": "6-7 天",
        },
        {
            "name": "广东惠州·双月湾",
            "season": "初夏海水温度最适合浮潜",
            "access": "深圳 → 双月湾 自驾 2h20",
            "duration": "2 天 1 晚",
        },
    ],
    7: [
        {
            "name": "新疆伊犁·薰衣草 + 喀拉峻草原",
            "season": "盛夏限定·薰衣草花海 + 高原湿润草甸",
            "access": "深圳宝安 → 伊宁 直飞 6h；包车环线 5 天",
            "duration": "7-8 天",
        },
        {
            "name": "贵州·荔波小七孔",
            "season": "7 月水量最大，喀斯特翡翠瀑布最美",
            "access": "深圳北 → 贵阳 高铁 5h30；或直飞 2h；转动车至荔波",
            "duration": "3-4 天",
        },
    ],
    8: [
        {
            "name": "川西·色达 + 稻城亚丁",
            "season": "盛夏高原凉爽 20℃，雪山草原并存",
            "access": "深圳 → 成都 2h40，包车走川西环线",
            "duration": "7-9 天",
        },
        {
            "name": "新疆·喀纳斯 + 禾木",
            "season": "童话秋初序章，湖水蓝得不真实",
            "access": "深圳 → 乌鲁木齐 5h30，转机至喀纳斯 1h",
            "duration": "6-7 天",
        },
    ],
    9: [
        {
            "name": "新疆·北疆大环线（金秋序章）",
            "season": "9 月下旬金秋第一缕，避开旺季",
            "access": "深圳 → 乌鲁木齐 直飞 5h30；租车自驾",
            "duration": "8-10 天",
        },
        {
            "name": "西藏·林芝 + 然乌湖",
            "season": "秋日转山转水，天高云淡",
            "access": "深圳 → 拉萨 中转 7-8h；或广州直飞林芝",
            "duration": "6-8 天",
        },
    ],
    10: [
        {
            "name": "新疆·喀纳斯金秋",
            "season": "10 月初金秋盛期，童话色彩最浓",
            "access": "深圳 → 乌鲁木齐 5h30 + 转喀纳斯 1h",
            "duration": "6-7 天",
        },
        {
            "name": "广东连州·地下河 + 小北江",
            "season": "国庆后错峰，山林秋色 + 喀斯特河谷",
            "access": "深圳北 → 连州 高铁 3h",
            "duration": "3 天",
        },
        {
            "name": "内蒙古·额济纳胡杨林",
            "season": "10 月中下旬金色限定·中国最壮阔的秋色",
            "access": "深圳 → 额济纳机场 中转 8h；建议跟团",
            "duration": "5-6 天",
        },
    ],
    11: [
        {
            "name": "云南·腾冲银杏村",
            "season": "11 月中下旬限定金黄，古村 + 火山温泉",
            "access": "深圳 → 腾冲 直飞 3h；或飞昆明转机",
            "duration": "4-5 天",
        },
        {
            "name": "广东梅州·雁南飞 + 客家围屋",
            "season": "初冬柚子成熟，客家美食季",
            "access": "深圳北 → 梅州西 高铁 3h10",
            "duration": "2-3 天",
        },
    ],
    12: [
        {
            "name": "哈尔滨·冰雪大世界（首开）",
            "season": "12 月下旬开园，跨年首选",
            "access": "深圳 → 哈尔滨直飞 5h",
            "duration": "4-5 天",
        },
        {
            "name": "海南·万宁日月湾冲浪",
            "season": "冬日浪况最佳，水温 24℃",
            "access": "深圳 → 三亚 1h40，转日月湾包车 2h",
            "duration": "4-5 天",
        },
        {
            "name": "广东从化·温泉 + 流溪河森林公园",
            "season": "冬日温泉季，深圳出发最近的温泉",
            "access": "深圳 → 从化 自驾 2h；或高铁至广州转车",
            "duration": "2 天 1 晚",
        },
    ],
}


def collect_travel(limit: int = 3) -> list[dict[str, str]]:
    pool = TRAVEL_BY_MONTH.get(datetime.now().month, [])
    picks = random.sample(pool, k=min(limit, len(pool))) if pool else []
    out = []
    for p in picks:
        kw = p["name"].split("·", 1)[0].strip()
        out.append({
            **p,
            "link": f"https://www.xiaohongshu.com/search_result?keyword={urllib.parse.quote(kw + ' 攻略')}&source=web_search_result_notes",
        })
    return out


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

def e(text: str) -> str:
    return html_mod.escape(text or "", quote=True)


def _link_or_span(href: str, inner_html: str, extra_class: str = "") -> str:
    """
    有 link 就包 <a>（可点击），否则包 <div>（静态）。
    extra_class 支持写入额外 HTML 属性（比如 `meme data-hidden="1"`）。
    """
    cls_and_attrs = extra_class.strip()
    # 兼容原逻辑：把 data-* 之前的都当作 class，之后都当属性
    if " data-" in " " + cls_and_attrs:
        cls_part, _, attr_part = cls_and_attrs.partition(" data-")
        attr_part = " data-" + attr_part
    else:
        cls_part, attr_part = cls_and_attrs, ""
    cls = f"row {cls_part}".strip()
    if href:
        return (
            f'<a class="{cls}"{attr_part} href="{e(href)}" target="_blank" rel="noopener">'
            f'{inner_html}<span class="chev" aria-hidden="true">›</span>'
            "</a>"
        )
    return f'<div class="{cls} static"{attr_part}>{inner_html}</div>'


def render_html(ctx: dict[str, Any]) -> str:
    date: datetime = ctx["date"]
    weekday = "一二三四五六日"[date.weekday()]
    date_str = f"{date:%Y 年 %m 月 %d 日} · 星期{weekday}"

    # ---- 分类热搜（每类默认折叠 4 条）----
    hot_blocks = []
    for cat_idx, (cat, items) in enumerate((ctx["hot"] or {}).items()):
        rows = []
        for i, m in enumerate(items, 1):
            # top3 排名加 top 类标识
            rank_cls = "rank top" if i <= 3 else "rank"
            hidden = ' data-hidden="1"' if i > 4 else ""
            inner = (
                f'<span class="{rank_cls}">{i:02d}</span>'
                '<div class="main">'
                f'<div class="title">{e(m["title"])}</div>'
                f'<div class="sub">{e(m["source"])}</div>'
                '</div>'
            )
            rows.append(_link_or_span(m.get("link", ""), inner, f"meme{hidden}"))
        extra = len(items) - 4
        expand_btn = (
            f'<button class="expand-btn" data-expanded="0">'
            f'<span class="expand-label">展开剩余 {extra} 条</span>'
            f'<span class="expand-chev">▾</span></button>'
        ) if extra > 0 else ""
        hot_blocks.append(
            f'<div class="cat-group" data-cat-idx="{cat_idx}">'
            f'  <div class="cat-title">'
            f'    <span class="cat-bar"></span>'
            f'    <span class="cat-name">{e(cat)}</span>'
            f'    <span class="cat-count">{len(items)}</span>'
            f'  </div>'
            f'  <div class="cat-items">{"".join(rows)}</div>'
            f'  {expand_btn}'
            f'</div>'
        )
    hot_section = "".join(hot_blocks) if hot_blocks else "<div class=\"row static\"><div class=\"main\"><div class=\"title\">今日热搜暂未抓取到</div></div></div>"

    # ---- 冷笑话 ----
    joke_items = "\n".join(f'<div class="joke">{e(j)}</div>' for j in ctx["jokes"])

    # ---- 影音（电影+电视剧+音乐）----
    def _render_media_row(m: dict[str, str]) -> str:
        poster = (
            f'<img src="{e(m["cover"])}" alt="" class="poster" loading="lazy">'
            if m.get("cover") else '<div class="poster poster-empty">♪</div>'
        )
        meta = f'<div class="meta">{e(m["meta"])}</div>' if m.get("meta") else ""
        inner = (
            f'{poster}'
            '<div class="main">'
            f'<div class="title">{e(m["title"])}</div>'
            f'<div class="sub"><span class="accent">{e(m["subtitle"])}</span></div>'
            f'{meta}'
            f'<div class="tag">{e(m["kind"])}</div>'
            '</div>'
        )
        return _link_or_span(m.get("link", ""), inner, "movie")

    media = ctx["media"] or {}
    all_media = (media.get("movies") or []) + (media.get("tvs") or []) + (media.get("musics") or [])
    media_items = "\n".join(_render_media_row(m) for m in all_media)

    # ---- 演唱会（主要艺人）----
    concert_rows = []
    for c in ctx["concerts"]:
        inner = (
            '<span class="dot concert-dot"></span>'
            '<div class="main">'
            f'<div class="title">{e(c["artist"])}<span class="chip">{e(c["tag"])}</span></div>'
            f'<div class="sub">{e(c["style"])} · 大麦网搜索最新场次</div>'
            '</div>'
        )
        concert_rows.append(_link_or_span(c.get("link", ""), inner, "concert"))
    concert_items = "\n".join(concert_rows)

    # ---- 旅游（详细版）----
    travel_rows = []
    for t in ctx["travels"]:
        why_sz = f'<div class="meta sz">深圳视角：{e(t["why_sz"])}</div>' if t.get("why_sz") else ""
        inner = (
            '<span class="dot travel-dot"></span>'
            '<div class="main">'
            f'<div class="title">{e(t["name"])}</div>'
            f'<div class="sub"><span class="accent">✦ {e(t["season"])}</span></div>'
            f'<div class="meta">🚄 {e(t["access"])}</div>'
            f'<div class="meta">⏱ 建议 {e(t.get("duration", ""))}</div>'
            f'{why_sz}'
            '</div>'
        )
        travel_rows.append(_link_or_span(t.get("link", ""), inner, "travel"))
    travel_items = "\n".join(travel_rows)

    # ---- 60 秒读世界（可点击跳百度搜索）----
    news_rows = []
    for n in (ctx["news"] or [])[:10]:
        inner = (
            '<span class="news-bullet"></span>'
            f'<div class="main"><div class="title news-title">{e(n["text"])}</div></div>'
        )
        news_rows.append(_link_or_span(n.get("link", ""), inner, "news-row"))
    news_items = "\n".join(news_rows)

    generated = f"{datetime.now():%Y-%m-%d %H:%M}"

    # 目录锚点项
    toc_items = [
        ("hot",      "🔥", "热搜"),
        ("jokes",    "😄", "冷笑话"),
        ("media",    "🎬", "影音"),
        ("concerts", "🎤", "演唱会"),
        ("travels",  "🧳", "旅游"),
        ("news",     "📰", "60 秒"),
    ]
    toc_html = "".join(
        f'<a class="toc-item" href="#{sid}" data-target="{sid}">'
        f'<span class="toc-emoji">{emoji}</span>'
        f'<span class="toc-label">{label}</span></a>'
        for sid, emoji, label in toc_items
    )

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="theme-color" content="#0d1424">
<title>每日热梗·{e(date.strftime('%Y-%m-%d'))}</title>
<style>
  :root {{
    --bg: #08091a;
    --bg-2: #0d0f24;
    --bg-soft: #151d33;
    --card: rgba(16,18,40,0.55);
    --card-border: rgba(255,255,255,0.08);
    --card-hover: rgba(255,255,255,0.06);
    --fg: #eef1f8;
    --fg-2: #b4bdd1;
    --fg-3: #7d879f;
    --accent: #ffb86b;
    --accent-2: #ffd93d;
    --accent-soft: rgba(255,184,107,0.14);
    --hot: #ff7a7a;
    --mint: #6ddaa8;
    --sky: #82b1ff;
    --pink: #ff8fb1;
    --purple: #b68aff;
    --divider: rgba(255,255,255,0.06);
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ margin: 0; padding: 0; }}
  html {{ scroll-behavior: smooth; scroll-padding-top: 70px; }}
  body {{
    font: 15px/1.65 -apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft Yahei", sans-serif;
    color: var(--fg);
    background:
      radial-gradient(ellipse 1200px 800px at 50% -10%, #0c0d20 0%, #05051a 60%, #030312 100%);
    min-height: 100vh;
    padding: 20px 16px calc(80px + env(safe-area-inset-bottom));
    -webkit-font-smoothing: antialiased;
    text-rendering: optimizeLegibility;
    overflow-x: hidden;
    position: relative;
  }}

  /* ========== 星空 Canvas ========== */
  #starfield {{
    position: fixed;
    inset: 0;
    z-index: -2;
    pointer-events: none;
  }}

  /* ========== 聚光灯遮罩：鼠标出现处"照亮"，其余压暗 ========== */
  .spotlight {{
    position: fixed;
    inset: 0;
    z-index: -1;
    pointer-events: none;
    background: radial-gradient(
      circle 280px at var(--mx, -500px) var(--my, -500px),
      transparent 0%,
      rgba(0,0,0,0.15) 40%,
      rgba(0,0,0,0.55) 100%);
    transition: opacity 0.4s ease;
  }}
  /* 柔金光晕层：鼠标位置附近一层暖色光 */
  .spotlight::before {{
    content: '';
    position: absolute;
    inset: 0;
    background: radial-gradient(
      circle 220px at var(--mx, -500px) var(--my, -500px),
      rgba(255,184,107,0.10) 0%,
      rgba(255,184,107,0.04) 40%,
      transparent 70%);
    mix-blend-mode: screen;
  }}

  /* ========== 顶部滚动进度条 ========== */
  .scroll-progress {{
    position: fixed;
    top: 0; left: 0;
    height: 2px;
    width: 0;
    background: linear-gradient(90deg, var(--hot), var(--accent), var(--accent-2));
    z-index: 100;
    transition: width 0.1s linear;
    box-shadow: 0 0 8px rgba(255,184,107,0.6);
  }}

  .wrap {{ max-width: 640px; margin: 0 auto; position: relative; }}
  a {{ color: inherit; text-decoration: none; -webkit-tap-highlight-color: transparent; }}

  /* ========== Header ========== */
  header {{
    padding: 4px 4px 16px;
    opacity: 0;
    animation: fadeInUp 0.7s cubic-bezier(.2,.8,.2,1) 0.05s forwards;
  }}
  .brand {{
    display: inline-flex; align-items: center; gap: 6px;
    font-size: 11px; letter-spacing: 3px;
    color: var(--fg-3); text-transform: uppercase;
    padding: 4px 10px;
    border: 1px solid var(--card-border);
    border-radius: 20px;
    backdrop-filter: blur(10px);
  }}
  h1 {{
    margin: 14px 0 4px;
    font-size: 28px; font-weight: 800; letter-spacing: -0.5px;
    background: linear-gradient(135deg, #fff 0%, #ffd9a8 100%);
    -webkit-background-clip: text;
    background-clip: text;
    -webkit-text-fill-color: transparent;
  }}
  .date {{
    color: var(--fg-3); font-size: 13px;
    font-family: "SF Mono", "Menlo", monospace;
  }}

  /* ========== 顶部目录锚点条 ========== */
  .toc {{
    position: sticky;
    top: 8px;
    z-index: 50;
    margin: 18px -4px 22px;
    padding: 8px;
    display: flex;
    gap: 6px;
    overflow-x: auto;
    overflow-y: hidden;
    scrollbar-width: none;
    background: rgba(11,16,32,0.7);
    backdrop-filter: saturate(180%) blur(18px);
    -webkit-backdrop-filter: saturate(180%) blur(18px);
    border: 1px solid var(--card-border);
    border-radius: 14px;
    box-shadow: 0 8px 28px -12px rgba(0,0,0,0.5);
    opacity: 0;
    animation: fadeInUp 0.7s cubic-bezier(.2,.8,.2,1) 0.15s forwards;
  }}
  .toc::-webkit-scrollbar {{ display: none; }}
  .toc-item {{
    flex: 0 0 auto;
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 7px 12px;
    font-size: 13px;
    font-weight: 500;
    color: var(--fg-2);
    border-radius: 10px;
    transition: all 0.2s ease;
    white-space: nowrap;
    position: relative;
  }}
  .toc-item:hover {{
    color: var(--fg);
    background: rgba(255,255,255,0.05);
    transform: translateY(-1px);
  }}
  .toc-item.active {{
    color: var(--accent);
    background: var(--accent-soft);
    box-shadow: 0 2px 10px -2px rgba(255,184,107,0.35);
  }}
  .toc-emoji {{ font-size: 14px; filter: saturate(1.1); }}

  /* ========== Section ========== */
  section {{
    margin-top: 18px;
    position: relative;
    background: rgba(12,14,28,0.55);
    border: 1px solid var(--card-border);
    border-radius: 18px;
    padding: 4px 0;
    overflow: hidden;
    backdrop-filter: blur(14px) saturate(1.2);
    -webkit-backdrop-filter: blur(14px) saturate(1.2);
    box-shadow: 0 24px 50px -30px rgba(0,0,0,0.7);
    opacity: 0;
    transform: translateY(18px);
    transition: opacity 0.7s cubic-bezier(.2,.8,.2,1),
                transform 0.7s cubic-bezier(.2,.8,.2,1),
                border-color 0.3s ease,
                box-shadow 0.3s ease;
  }}
  section:hover {{
    border-color: rgba(255,184,107,0.28);
    box-shadow:
      0 24px 60px -30px rgba(0,0,0,0.75),
      0 0 30px -10px rgba(255,184,107,0.18);
  }}
  section.revealed {{ opacity: 1; transform: translateY(0); }}
  section.collapsed .sec-body {{ display: none; }}
  section.collapsed .sec-toggle {{ transform: rotate(-90deg); }}

  .sec-head {{
    display: flex; align-items: center;
    padding: 16px 18px 10px;
    gap: 10px;
    cursor: pointer;
    user-select: none;
    position: relative;
  }}
  .sec-emoji {{
    font-size: 18px;
    display: inline-block;
    transition: transform 0.3s cubic-bezier(.5,-0.5,.3,1.5);
  }}
  section.revealed .sec-emoji {{
    animation: pop 0.6s cubic-bezier(.34,1.56,.64,1);
  }}
  @keyframes pop {{
    0%   {{ transform: scale(0.6) rotate(-10deg); }}
    60%  {{ transform: scale(1.25) rotate(6deg); }}
    100% {{ transform: scale(1) rotate(0); }}
  }}
  .sec-head h2 {{
    margin: 0;
    font-size: 14px; font-weight: 700;
    letter-spacing: 1.5px;
    color: var(--fg);
  }}
  .sec-head .count {{
    margin-left: auto;
    color: var(--fg-3); font-size: 12px;
    font-family: "SF Mono", "Menlo", monospace;
  }}
  .sec-toggle {{
    margin-left: 10px;
    width: 24px; height: 24px;
    display: grid; place-items: center;
    border-radius: 50%;
    color: var(--fg-3);
    transition: transform 0.3s cubic-bezier(.2,.8,.2,1), background 0.2s ease;
    font-size: 14px;
  }}
  .sec-head:hover .sec-toggle {{ background: rgba(255,255,255,0.08); color: var(--accent); }}

  /* ========== Row ========== */
  .row {{
    display: flex; align-items: center; gap: 12px;
    padding: 14px 18px;
    border-top: 1px solid var(--divider);
    transition: background 0.2s ease, transform 0.25s ease, box-shadow 0.25s ease;
    position: relative;
    overflow: hidden;
  }}
  .row[data-hidden="1"] {{ display: none; }}
  .row.show-all[data-hidden="1"] {{ display: flex; animation: fadeInUp 0.4s ease both; }}
  a.row {{ cursor: pointer; }}
  a.row:hover {{
    background: var(--card-hover);
    transform: translateY(-2px);
    box-shadow: 0 12px 24px -14px rgba(0,0,0,0.5),
                0 0 0 1px rgba(255,255,255,0.04) inset;
  }}
  a.row:active {{ transform: translateY(0); transition: transform 0.08s; }}
  /* 高光扫过 */
  a.row::before {{
    content: '';
    position: absolute;
    inset: 0;
    background: linear-gradient(120deg,
      transparent 30%,
      rgba(255,255,255,0.08) 50%,
      transparent 70%);
    transform: translateX(-120%);
    transition: transform 0.7s ease;
    pointer-events: none;
  }}
  a.row:hover::before {{ transform: translateX(120%); }}
  /* 点击涟漪 */
  .ripple {{
    position: absolute;
    border-radius: 50%;
    background: rgba(255,184,107,0.35);
    transform: translate(-50%, -50%) scale(0);
    animation: ripple 0.6s ease-out;
    pointer-events: none;
    z-index: 1;
  }}
  @keyframes ripple {{
    to {{ transform: translate(-50%, -50%) scale(4); opacity: 0; }}
  }}
  .row.static {{ cursor: default; }}
  .row .main {{ flex: 1; min-width: 0; position: relative; z-index: 2; }}
  .row .title {{
    font-weight: 600; font-size: 15px; color: var(--fg);
    line-height: 1.5; word-break: break-word;
  }}
  .row .sub {{
    margin-top: 3px;
    font-size: 12px; color: var(--fg-3);
  }}
  .row .sub .accent {{ color: var(--accent); font-weight: 600; }}
  .row .chev {{
    flex: 0 0 auto;
    color: var(--fg-3);
    font-size: 22px; line-height: 1;
    font-family: "SF Mono", "Menlo", monospace;
    opacity: 0.4;
    transition: transform 0.25s ease, opacity 0.25s ease, color 0.25s ease;
    position: relative; z-index: 2;
  }}
  a.row:hover .chev {{ opacity: 1; transform: translateX(3px); color: var(--accent); }}

  /* ========== 热梗：排名金属质感 ========== */
  .meme .rank {{
    flex: 0 0 28px;
    font-family: "SF Mono", "Menlo", monospace;
    font-weight: 800;
    font-size: 15px;
    color: var(--fg-3);
    letter-spacing: 0;
    text-align: left;
  }}
  .meme .rank.top {{
    background: linear-gradient(135deg, #ff5a5a 0%, #ffb86b 50%, #ffd93d 100%);
    -webkit-background-clip: text;
    background-clip: text;
    -webkit-text-fill-color: transparent;
    filter: drop-shadow(0 1px 3px rgba(255,122,122,0.35));
  }}

  /* ========== 冷笑话 ========== */
  .joke {{
    padding: 14px 18px;
    border-top: 1px solid var(--divider);
    color: var(--fg);
    font-size: 14.5px;
    line-height: 1.75;
    position: relative;
    transition: background 0.2s ease;
  }}
  .joke:hover {{ background: rgba(255,255,255,0.02); }}
  .joke::before {{
    content: '"';
    display: inline-block;
    background: linear-gradient(135deg, var(--accent), var(--accent-2));
    -webkit-background-clip: text;
    background-clip: text;
    -webkit-text-fill-color: transparent;
    font-size: 30px;
    line-height: 0.8;
    margin-right: 6px;
    font-weight: 700;
    vertical-align: -8px;
  }}

  /* ========== 影音 ========== */
  .movie .poster {{
    flex: 0 0 64px;
    width: 64px; height: 88px;
    object-fit: cover;
    border-radius: 10px;
    background: var(--bg-soft);
    box-shadow: 0 6px 16px -6px rgba(0,0,0,0.6);
    transition: transform 0.3s ease;
  }}
  a.row.movie:hover .poster {{ transform: scale(1.05) rotate(-1deg); }}
  .movie .poster-empty {{
    display: grid; place-items: center;
    color: var(--fg-3); font-size: 24px;
  }}
  .movie .main {{ display: flex; flex-direction: column; gap: 4px; min-height: 88px; }}
  .movie .title {{ font-size: 16px; font-weight: 700; }}
  .movie .meta {{ font-size: 12px; color: var(--fg-3); }}
  .movie .tag {{
    align-self: flex-start;
    margin-top: auto;
    padding: 2px 8px;
    background: var(--accent-soft);
    color: var(--accent);
    font-size: 11px;
    border-radius: 999px;
    font-weight: 600;
  }}

  /* ========== 演唱会 & 旅游 ========== */
  .dot {{
    flex: 0 0 8px;
    width: 8px; height: 8px;
    border-radius: 50%;
    margin-left: 4px;
    margin-top: 10px;
    align-self: flex-start;
    transition: box-shadow 0.25s ease, transform 0.25s ease;
  }}
  .concert-dot {{ background: var(--sky); box-shadow: 0 0 10px rgba(130,177,255,0.5); }}
  .travel-dot  {{ background: var(--mint); box-shadow: 0 0 10px rgba(109,218,168,0.5); }}
  a.row:hover .concert-dot {{ box-shadow: 0 0 16px rgba(130,177,255,0.9); transform: scale(1.3); }}
  a.row:hover .travel-dot {{ box-shadow: 0 0 16px rgba(109,218,168,0.9); transform: scale(1.3); }}
  .chip {{
    display: inline-block;
    margin-left: 8px;
    padding: 1px 8px;
    border-radius: 999px;
    background: rgba(130,177,255,0.15);
    color: var(--sky);
    font-size: 11px;
    font-weight: 500;
    vertical-align: 2px;
  }}
  .meta {{ font-size: 12px; color: var(--fg-3); margin-top: 4px; line-height: 1.6; }}
  .meta.sz {{ color: var(--mint); }}

  /* ========== 分类热搜分组 ========== */
  .cat-group {{
    border-top: 1px solid var(--divider);
    position: relative;
  }}
  .cat-group:first-of-type {{ border-top: 1px solid var(--divider); }}
  .cat-title {{
    padding: 14px 18px 8px;
    font-size: 13px;
    font-weight: 700;
    color: var(--fg-2);
    letter-spacing: 1px;
    display: flex;
    align-items: center;
    gap: 10px;
    position: relative;
  }}
  /* 分类左侧色条 */
  .cat-bar {{
    width: 3px;
    height: 14px;
    border-radius: 2px;
    background: var(--accent);
    transition: height 0.25s cubic-bezier(.2,.8,.2,1);
  }}
  .cat-group[data-cat-idx="0"] .cat-bar {{ background: var(--sky); }}
  .cat-group[data-cat-idx="1"] .cat-bar {{ background: var(--pink); }}
  .cat-group[data-cat-idx="2"] .cat-bar {{ background: var(--mint); }}
  .cat-group[data-cat-idx="3"] .cat-bar {{ background: var(--purple); }}
  .cat-group[data-cat-idx="4"] .cat-bar {{ background: var(--accent); }}
  .cat-group[data-cat-idx="5"] .cat-bar {{ background: var(--hot); }}
  .cat-group[data-cat-idx="6"] .cat-bar {{ background: var(--accent-2); }}
  .cat-group[data-cat-idx="7"] .cat-bar {{ background: var(--mint); }}
  .cat-group:hover .cat-bar {{ height: 20px; }}
  .cat-count {{
    font-family: "SF Mono", "Menlo", monospace;
    font-size: 11px;
    color: var(--fg-3);
    background: rgba(255,255,255,0.05);
    padding: 1px 8px;
    border-radius: 999px;
    font-weight: 500;
    margin-left: auto;
  }}
  .expand-btn {{
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 4px;
    margin: 4px 16px 12px;
    padding: 8px 14px;
    width: calc(100% - 32px);
    background: transparent;
    border: 1px dashed var(--card-border);
    border-radius: 10px;
    color: var(--fg-3);
    font-size: 12.5px;
    cursor: pointer;
    transition: all 0.2s ease;
  }}
  .expand-btn:hover {{
    color: var(--accent);
    border-color: var(--accent);
    background: var(--accent-soft);
  }}
  .expand-chev {{
    display: inline-block;
    transition: transform 0.3s cubic-bezier(.2,.8,.2,1);
    font-size: 11px;
  }}
  .expand-btn[data-expanded="1"] .expand-chev {{ transform: rotate(180deg); }}

  /* ========== 60 秒 ========== */
  .news-row .news-title {{
    font-weight: 500;
    color: var(--fg-2);
    font-size: 13.5px;
    line-height: 1.7;
  }}
  .news-bullet {{
    flex: 0 0 6px;
    width: 6px; height: 6px;
    background: var(--fg-3);
    border-radius: 50%;
    align-self: flex-start;
    margin-top: 10px;
    transition: background 0.2s ease, box-shadow 0.2s ease;
  }}
  a.row:hover .news-bullet {{ background: var(--accent); box-shadow: 0 0 10px var(--accent); }}

  /* ========== 回到顶部按钮 ========== */
  .back-top {{
    position: fixed;
    right: 18px;
    bottom: calc(24px + env(safe-area-inset-bottom));
    width: 44px; height: 44px;
    border: 1px solid var(--card-border);
    border-radius: 50%;
    background: rgba(11,16,32,0.8);
    backdrop-filter: blur(12px);
    color: var(--accent);
    font-size: 20px;
    cursor: pointer;
    display: grid; place-items: center;
    opacity: 0;
    transform: translateY(20px) scale(0.8);
    transition: all 0.3s cubic-bezier(.2,.8,.2,1);
    pointer-events: none;
    z-index: 60;
    box-shadow: 0 10px 30px -10px rgba(0,0,0,0.5);
  }}
  .back-top.show {{ opacity: 1; transform: translateY(0) scale(1); pointer-events: auto; }}
  .back-top:hover {{ background: var(--accent); color: #0b1020; transform: translateY(-4px) scale(1.05); }}

  /* ========== Footer ========== */
  footer {{
    margin-top: 32px;
    text-align: center;
    color: var(--fg-3);
    font-size: 12px;
    line-height: 2;
    font-family: "SF Mono", "Menlo", monospace;
  }}
  footer .sources {{ color: var(--fg-3); }}
  footer a {{ color: var(--accent); transition: color 0.2s; }}
  footer a:hover {{ color: var(--accent-2); }}

  /* ========== 动画关键帧 ========== */
  @keyframes fadeInUp {{
    from {{ opacity: 0; transform: translateY(16px); }}
    to   {{ opacity: 1; transform: translateY(0); }}
  }}

  /* 用户偏好：减少动画 */
  @media (prefers-reduced-motion: reduce) {{
    *, *::before, *::after {{
      animation-duration: 0.01s !important;
      animation-iteration-count: 1 !important;
      transition-duration: 0.01s !important;
    }}
    #starfield, .spotlight {{ display: none; }}
  }}

  /* 移动端：没鼠标，聚光灯直接关掉 */
  @media (hover: none) and (pointer: coarse) {{
    .spotlight {{ display: none; }}
  }}
</style>
</head>
<body>
<div class="scroll-progress" id="scrollProgress"></div>

<canvas id="starfield" aria-hidden="true"></canvas>
<div class="spotlight" aria-hidden="true"></div>

<div class="wrap">
  <header>
    <div class="brand">Daily Digest</div>
    <h1>今日热梗 · 生活资讯</h1>
    <div class="date">{e(date_str)}</div>
  </header>

  <nav class="toc" aria-label="目录">{toc_html}</nav>

  <section id="hot" data-sec>
    <div class="sec-head">
      <span class="sec-emoji">🔥</span>
      <h2>今日热搜</h2>
      <div class="count">微博 · 抖音 · 小红书 · B 站</div>
      <span class="sec-toggle" aria-hidden="true">▾</span>
    </div>
    <div class="sec-body">
      {hot_section}
    </div>
  </section>

  <section id="jokes" data-sec>
    <div class="sec-head">
      <span class="sec-emoji">😄</span>
      <h2>冷笑话</h2>
      <div class="count">{len(ctx["jokes"])} 条</div>
      <span class="sec-toggle" aria-hidden="true">▾</span>
    </div>
    <div class="sec-body">
      {joke_items}
    </div>
  </section>

  <section id="media" data-sec>
    <div class="sec-head">
      <span class="sec-emoji">🎬</span>
      <h2>影音榜单</h2>
      <div class="count">电影 · 剧集 · 流行音乐</div>
      <span class="sec-toggle" aria-hidden="true">▾</span>
    </div>
    <div class="sec-body">
      {media_items}
    </div>
  </section>

  <section id="concerts" data-sec>
    <div class="sec-head">
      <span class="sec-emoji">🎤</span>
      <h2>演唱会关注</h2>
      <div class="count">{len(ctx["concerts"])} 位 · 大麦网最新场次</div>
      <span class="sec-toggle" aria-hidden="true">▾</span>
    </div>
    <div class="sec-body">
      {concert_items}
    </div>
  </section>

  <section id="travels" data-sec>
    <div class="sec-head">
      <span class="sec-emoji">🧳</span>
      <h2>旅游推荐</h2>
      <div class="count">{len(ctx["travels"])} 处 · 深圳出发视角</div>
      <span class="sec-toggle" aria-hidden="true">▾</span>
    </div>
    <div class="sec-body">
      {travel_items}
    </div>
  </section>

  <section id="news" data-sec>
    <div class="sec-head">
      <span class="sec-emoji">📰</span>
      <h2>60 秒读懂世界</h2>
      <div class="count">{len(ctx["news"] or [])} 条 · 点击搜索详情</div>
      <span class="sec-toggle" aria-hidden="true">▾</span>
    </div>
    <div class="sec-body">
      {news_items}
    </div>
  </section>

  <footer>
    <div class="sources">微博 · 抖音 · 小红书 · B 站 · 豆瓣 · 网易云 · 60s</div>
    <div>生成于 {e(generated)} · <a href="./archive.html">📚 往期归档</a></div>
  </footer>
</div>

<button class="back-top" id="backTop" aria-label="回到顶部">↑</button>

<script>
(function() {{
  // ---------- 1) 滚动进度条 ----------
  var progress = document.getElementById('scrollProgress');
  var backTop = document.getElementById('backTop');
  function updateScroll() {{
    var h = document.documentElement;
    var scrolled = h.scrollTop;
    var max = h.scrollHeight - h.clientHeight;
    var pct = max > 0 ? (scrolled / max * 100) : 0;
    progress.style.width = pct + '%';
    if (scrolled > 600) backTop.classList.add('show');
    else backTop.classList.remove('show');
  }}
  window.addEventListener('scroll', updateScroll, {{ passive: true }});
  updateScroll();

  // ---------- 2) 回到顶部 ----------
  backTop.addEventListener('click', function() {{
    window.scrollTo({{ top: 0, behavior: 'smooth' }});
  }});

  // ---------- 3) Section 入场动画 + TOC 高亮联动 ----------
  var sections = document.querySelectorAll('section[data-sec]');
  var tocItems = document.querySelectorAll('.toc-item');
  var io = new IntersectionObserver(function(entries) {{
    entries.forEach(function(entry) {{
      if (entry.isIntersecting) {{
        entry.target.classList.add('revealed');
      }}
    }});
  }}, {{ threshold: 0.08 }});
  sections.forEach(function(s) {{ io.observe(s); }});

  // 滚动联动 toc 高亮
  function updateToc() {{
    var pos = window.scrollY + 140;
    var active = null;
    sections.forEach(function(s) {{
      if (s.offsetTop <= pos) active = s.id;
    }});
    tocItems.forEach(function(t) {{
      t.classList.toggle('active', t.dataset.target === active);
    }});
  }}
  window.addEventListener('scroll', updateToc, {{ passive: true }});
  updateToc();

  // ---------- 4) Section 折叠 ----------
  document.querySelectorAll('.sec-head').forEach(function(head) {{
    head.addEventListener('click', function(ev) {{
      if (ev.target.closest('a, button')) return;
      head.parentElement.classList.toggle('collapsed');
    }});
  }});

  // ---------- 5) 热搜「展开全部」 ----------
  document.querySelectorAll('.expand-btn').forEach(function(btn) {{
    btn.addEventListener('click', function(ev) {{
      ev.stopPropagation();
      var group = btn.closest('.cat-group');
      var expanded = btn.dataset.expanded === '1';
      group.querySelectorAll('.row[data-hidden="1"]').forEach(function(r) {{
        r.classList.toggle('show-all', !expanded);
      }});
      btn.dataset.expanded = expanded ? '0' : '1';
      var label = btn.querySelector('.expand-label');
      if (label) {{
        label.textContent = expanded
          ? label.textContent.replace('收起', '展开剩余').replace(/^收起$/, '展开')
          : label.textContent.replace('展开剩余', '收起').replace(/^展开$/, '收起');
        // 更稳妥：直接替换
        var hiddenCount = group.querySelectorAll('.row[data-hidden="1"]').length;
        label.textContent = expanded ? ('展开剩余 ' + hiddenCount + ' 条') : '收起';
      }}
    }});
  }});

  // ---------- 6) 点击涟漪 ----------
  document.querySelectorAll('a.row').forEach(function(row) {{
    row.addEventListener('click', function(ev) {{
      var rect = row.getBoundingClientRect();
      var ripple = document.createElement('span');
      ripple.className = 'ripple';
      var size = Math.max(rect.width, rect.height) * 0.5;
      ripple.style.width = ripple.style.height = size + 'px';
      ripple.style.left = (ev.clientX - rect.left) + 'px';
      ripple.style.top = (ev.clientY - rect.top) + 'px';
      row.appendChild(ripple);
      setTimeout(function() {{ ripple.remove(); }}, 700);
    }});
  }});

  // ---------- 7) 星空 Canvas + 鼠标聚光灯 + 光圈内星座连线 ----------
  var canvas = document.getElementById('starfield');
  var spotlight = document.querySelector('.spotlight');
  var isTouch = window.matchMedia('(hover: none) and (pointer: coarse)').matches;
  var prefersReduced = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  if (canvas && !prefersReduced) {{
    var ctx2 = canvas.getContext('2d');
    var DPR = Math.min(window.devicePixelRatio || 1, 2);
    var W = 0, H = 0;
    var stars = [];
    var N = 150;
    var SPOT_R = 260;         // 聚光灯半径（逻辑像素）
    var LINK_DIST = 110;      // 连线最大距离
    var mouse = {{ x: -9999, y: -9999, active: false }};

    function resize() {{
      W = window.innerWidth;
      H = window.innerHeight;
      canvas.width  = W * DPR;
      canvas.height = H * DPR;
      canvas.style.width  = W + 'px';
      canvas.style.height = H + 'px';
      ctx2.setTransform(DPR, 0, 0, DPR, 0, 0);
    }}
    resize();
    window.addEventListener('resize', resize);

    function seed() {{
      stars = [];
      for (var i = 0; i < N; i++) {{
        stars.push({{
          x: Math.random() * W,
          y: Math.random() * H,
          r: Math.random() * 1.1 + 0.3,
          tw: Math.random() * Math.PI * 2,    // 闪烁相位
          sp: Math.random() * 0.02 + 0.008,   // 闪烁速度
          vx: (Math.random() - 0.5) * 0.18,
          vy: (Math.random() - 0.5) * 0.18,
        }});
      }}
    }}
    seed();
    window.addEventListener('resize', function() {{ seed(); }});

    // 鼠标跟踪（同时更新 spotlight CSS 变量）
    window.addEventListener('mousemove', function(ev) {{
      mouse.x = ev.clientX;
      mouse.y = ev.clientY;
      mouse.active = true;
      if (spotlight) {{
        spotlight.style.setProperty('--mx', ev.clientX + 'px');
        spotlight.style.setProperty('--my', ev.clientY + 'px');
      }}
    }}, {{ passive: true }});
    window.addEventListener('mouseleave', function() {{
      mouse.active = false;
      mouse.x = mouse.y = -9999;
      if (spotlight) {{
        spotlight.style.setProperty('--mx', '-500px');
        spotlight.style.setProperty('--my', '-500px');
      }}
    }});

    function tick() {{
      ctx2.clearRect(0, 0, W, H);

      // --- 1) 更新位置 + 环绕 ---
      for (var i = 0; i < N; i++) {{
        var s = stars[i];
        s.x += s.vx; s.y += s.vy;
        s.tw += s.sp;
        if (s.x < -5) s.x = W + 5;
        if (s.x > W + 5) s.x = -5;
        if (s.y < -5) s.y = H + 5;
        if (s.y > H + 5) s.y = -5;
      }}

      // --- 2) 画连线（仅鼠标聚光灯范围内的星星互相连接）---
      if (mouse.active) {{
        ctx2.lineWidth = 0.6;
        for (var i = 0; i < N; i++) {{
          var a = stars[i];
          var dxm = a.x - mouse.x, dym = a.y - mouse.y;
          if (dxm * dxm + dym * dym > SPOT_R * SPOT_R) continue;
          for (var j = i + 1; j < N; j++) {{
            var b = stars[j];
            var dxm2 = b.x - mouse.x, dym2 = b.y - mouse.y;
            if (dxm2 * dxm2 + dym2 * dym2 > SPOT_R * SPOT_R) continue;
            var dx = a.x - b.x, dy = a.y - b.y;
            var d2 = dx * dx + dy * dy;
            if (d2 < LINK_DIST * LINK_DIST) {{
              var d = Math.sqrt(d2);
              var alpha = (1 - d / LINK_DIST) * 0.5;
              ctx2.strokeStyle = 'rgba(255,200,140,' + alpha.toFixed(3) + ')';
              ctx2.beginPath();
              ctx2.moveTo(a.x, a.y);
              ctx2.lineTo(b.x, b.y);
              ctx2.stroke();
            }}
          }}
        }}
      }}

      // --- 3) 画星点 ---
      for (var i = 0; i < N; i++) {{
        var s = stars[i];
        // 闪烁亮度 0.25~0.9
        var base = 0.25 + (Math.sin(s.tw) + 1) * 0.5 * 0.5;

        // 聚光灯增亮
        var boost = 0;
        if (mouse.active) {{
          var dxm = s.x - mouse.x, dym = s.y - mouse.y;
          var d = Math.sqrt(dxm * dxm + dym * dym);
          if (d < SPOT_R) boost = (1 - d / SPOT_R) * 0.7;
          else base *= 0.55;  // 聚光灯外的星星压暗
        }}

        var alpha = Math.min(1, base + boost);
        var r = s.r * (1 + boost * 0.8);

        // 光晕
        var grad = ctx2.createRadialGradient(s.x, s.y, 0, s.x, s.y, r * 5);
        grad.addColorStop(0, 'rgba(255,240,215,' + alpha.toFixed(3) + ')');
        grad.addColorStop(0.4, 'rgba(180,200,235,' + (alpha * 0.25).toFixed(3) + ')');
        grad.addColorStop(1, 'rgba(160,180,230,0)');
        ctx2.fillStyle = grad;
        ctx2.beginPath();
        ctx2.arc(s.x, s.y, r * 5, 0, Math.PI * 2);
        ctx2.fill();

        // 实心小核
        ctx2.fillStyle = 'rgba(255,250,235,' + alpha.toFixed(3) + ')';
        ctx2.beginPath();
        ctx2.arc(s.x, s.y, r, 0, Math.PI * 2);
        ctx2.fill();
      }}

      requestAnimationFrame(tick);
    }}
    tick();
  }}

  // ---------- 8) 触屏设备：跟随手指作为"聚光灯" ----------
  if (isTouch && spotlight) {{
    window.addEventListener('touchmove', function(ev) {{
      var t = ev.touches[0]; if (!t) return;
      spotlight.style.setProperty('--mx', t.clientX + 'px');
      spotlight.style.setProperty('--my', t.clientY + 'px');
    }}, {{ passive: true }});
  }}
}})();
</script>
</body>
</html>
"""


def write_html_files(html: str, date: datetime) -> Path:
    """写入 docs/index.html + 归档 docs/YYYY-MM-DD.html + docs/archive.html 索引页。"""
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    archive = DOCS_DIR / f"{date:%Y-%m-%d}.html"
    index = DOCS_DIR / "index.html"
    archive.write_text(html, encoding="utf-8")
    index.write_text(html, encoding="utf-8")
    log.info("HTML 已生成: %s", archive.name)
    # 每次生成后更新归档索引
    write_archive_index()
    return index


def write_archive_index() -> None:
    """扫描 docs/ 下所有 YYYY-MM-DD.html，生成归档列表页 archive.html。"""
    entries: list[str] = []
    pattern = re.compile(r"^(\d{4}-\d{2}-\d{2})\.html$")
    dates: list[str] = []
    for p in sorted(DOCS_DIR.glob("*.html"), reverse=True):
        m = pattern.match(p.name)
        if not m:
            continue
        d = m.group(1)
        dates.append(d)
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            weekday = "一二三四五六日"[dt.weekday()]
            label = f"{dt:%Y 年 %m 月 %d 日}"
            sub = f"星期{weekday}"
        except ValueError:
            label, sub = d, ""
        entries.append(
            f'<a class="arch-row" href="./{d}.html">'
            f'<div class="main">'
            f'<div class="title">{label}</div>'
            f'<div class="sub">{sub}</div>'
            f'</div><span class="chev">›</span></a>'
        )

    if not entries:
        entries.append(
            '<div class="arch-row static"><div class="main">'
            '<div class="title">暂无归档</div></div></div>'
        )

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="theme-color" content="#0d1424">
<title>每日热梗 · 往期归档</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    font: 15px/1.6 -apple-system, BlinkMacSystemFont, "PingFang SC", "Microsoft Yahei", sans-serif;
    background: #0d1424;
    color: #eef1f8;
    background-image:
      radial-gradient(800px 400px at -10% -10%, rgba(130,177,255,0.08), transparent 60%),
      radial-gradient(600px 300px at 110% 0%, rgba(255,184,107,0.06), transparent 60%);
    min-height: 100vh;
    padding: 28px 16px 80px;
    -webkit-font-smoothing: antialiased;
  }}
  .wrap {{ max-width: 640px; margin: 0 auto; }}
  a {{ color: inherit; text-decoration: none; }}
  .brand {{
    display: inline-block;
    font-size: 11px; letter-spacing: 3px;
    color: #7d879f; text-transform: uppercase;
    padding: 4px 10px;
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 20px;
  }}
  h1 {{ margin: 14px 0 4px; font-size: 26px; font-weight: 800; }}
  .sub-top {{ color: #7d879f; font-size: 13px; margin-bottom: 24px; }}
  .back {{
    display: inline-flex; align-items: center; gap: 6px;
    color: #ffb86b; font-size: 13px;
    margin-bottom: 20px;
  }}
  .list {{
    background: rgba(255,255,255,0.035);
    border: 1px solid rgba(255,255,255,0.07);
    border-radius: 16px;
    overflow: hidden;
  }}
  .arch-row {{
    display: flex; align-items: center; gap: 10px;
    padding: 16px 18px;
    border-top: 1px solid rgba(255,255,255,0.06);
    transition: background 0.18s ease;
  }}
  .arch-row:first-child {{ border-top: none; }}
  a.arch-row:hover {{ background: rgba(255,255,255,0.06); }}
  .arch-row .main {{ flex: 1; }}
  .arch-row .title {{ font-weight: 600; font-size: 15px; }}
  .arch-row .sub {{ font-size: 12px; color: #7d879f; margin-top: 2px; }}
  .arch-row .chev {{
    color: #7d879f; font-size: 22px; opacity: 0.6;
    transition: transform 0.18s ease, color 0.18s ease;
  }}
  a.arch-row:hover .chev {{ color: #ffb86b; transform: translateX(2px); opacity: 1; }}
</style>
</head>
<body>
<div class="wrap">
  <div class="brand">Daily Digest · Archive</div>
  <h1>📚 往期归档</h1>
  <div class="sub-top">共 {len(dates)} 期</div>
  <a class="back" href="./">← 回到今日</a>
  <div class="list">
    {"".join(entries)}
  </div>
</div>
</body>
</html>
"""
    (DOCS_DIR / "archive.html").write_text(html, encoding="utf-8")


# ---------------------------------------------------------------------------
# 企微推送
# ---------------------------------------------------------------------------

def _truncate(text: str, n: int) -> str:
    text = (text or "").strip()
    return text if len(text) <= n else text[: n - 1] + "…"


def send_template_card(ctx: dict[str, Any], page_url: str) -> bool:
    """
    发送模板卡片（图文展示型），点击整卡或「查看全部」按钮跳转 HTML 详情页。
    相比 news 类型，template_card 带明确的跳转按钮，交互更清晰。
    """
    date: datetime = ctx["date"]
    weekday = "一二三四五六日"[date.weekday()]

    preview_rows: list[dict[str, str]] = []
    # 选一条最热的热搜作为预览（第一个分类的第一条）
    hot = ctx.get("hot") or {}
    first_cat = next(iter(hot.values()), None)
    if first_cat:
        preview_rows.append({
            "keyname": "🔥 热搜",
            "value": _truncate(first_cat[0]["title"], 22),
        })
    media = ctx.get("media") or {}
    movies = media.get("movies") or []
    musics = media.get("musics") or []
    if movies:
        preview_rows.append({
            "keyname": "🎬 影音",
            "value": _truncate(f"《{movies[0]['title']}》 {movies[0]['subtitle']}", 22),
        })
    elif musics:
        preview_rows.append({
            "keyname": "🎵 音乐",
            "value": _truncate(f"{musics[0]['title']} - {musics[0]['subtitle']}", 22),
        })
    if ctx.get("travels"):
        preview_rows.append({
            "keyname": "🧳 旅游",
            "value": _truncate(ctx["travels"][0]["name"], 22),
        })

    quote_text = ""
    if ctx.get("jokes"):
        quote_text = _truncate(ctx["jokes"][0], 80)

    payload = {
        "msgtype": "template_card",
        "template_card": {
            "card_type": "text_notice",
            "source": {
                "desc": "每日精选 · Daily Digest",
                "desc_color": 0,
            },
            "main_title": {
                "title": f"📬 今日精选·{date:%m 月 %d 日} 星期{weekday}",
                "desc": "热搜 / 冷笑话 / 影音 / 演唱会 / 旅游",
            },
            "horizontal_content_list": preview_rows,
            "jump_list": [{
                "type": 1,
                "url": page_url,
                "title": "🔗 查看完整详情",
            }],
            "card_action": {
                "type": 1,
                "url": page_url,
            },
        },
    }
    if quote_text:
        payload["template_card"]["quote_area"] = {
            "type": 0,
            "title": "😄 今日份冷笑话",
            "quote_text": quote_text,
        }

    try:
        resp = http_post_json(WEBHOOK_URL, payload)
    except Exception as exc:
        log.error("Webhook 请求失败: %s", exc)
        return False

    if resp.get("errcode") == 0:
        log.info("✅ 推送成功（模板卡片 → %s）", page_url)
        return True
    log.error("❌ 推送失败: %s", resp)
    return False


# 保留旧函数名作为别名（兼容）
send_news_card = send_template_card


def send_markdown_fallback(ctx: dict[str, Any]) -> bool:
    """未配置 PAGES_URL 时的降级方案——继续发 markdown。"""
    date: datetime = ctx["date"]
    weekday = "一二三四五六日"[date.weekday()]
    lines = [f"# 📬 每日精选·{date:%Y-%m-%d} 星期{weekday}\n"]

    lines.append("\n## 🔥 今日热搜")
    count = 0
    for cat, items in (ctx.get("hot") or {}).items():
        lines.append(f"\n**{cat}**")
        for m in items[:3]:
            lines.append(f"- {m['title']}  `{m['source']}`")
            count += 1
            if count >= 15:
                break
        if count >= 15:
            break

    lines.append("\n## 😄 冷笑话")
    for j in ctx.get("jokes", [])[:3]:
        lines.append(f"- {j}")

    lines.append("\n## 🎬 影音")
    media = ctx.get("media") or {}
    for m in (media.get("movies") or [])[:2]:
        lines.append(f"- 《{m['title']}》· {m['subtitle']}")
    for m in (media.get("musics") or [])[:2]:
        lines.append(f"- 🎵 {m['title']} - {m['subtitle']}")

    lines.append("\n## 🎤 演唱会关注")
    for c in ctx.get("concerts", []):
        lines.append(f"- {c['artist']}（{c['tag']}）")

    lines.append("\n## 🧳 旅游推荐（深圳出发）")
    for t in ctx.get("travels", []):
        lines.append(f"- **{t['name']}**｜{t.get('season', '')}")

    lines.append("\n> <font color=\"comment\">云端部署后会收到精美 HTML 页面链接 ✨</font>")

    try:
        resp = http_post_json(
            WEBHOOK_URL,
            {"msgtype": "markdown", "markdown": {"content": "\n".join(lines)}},
        )
    except Exception as exc:
        log.error("Webhook 请求失败: %s", exc)
        return False
    return resp.get("errcode") == 0


# ---------------------------------------------------------------------------
# 企业微信智能机器人（长连接）主动推送
# ---------------------------------------------------------------------------

WS_URL = "wss://openws.work.weixin.qq.com"
_ws_lock = threading.Lock()


def _ws_send(ws, obj):
    with _ws_lock:
        ws.send(json.dumps(obj))


def _send_cmd(ws, cmd: str, body: dict[str, Any], timeout: int = 30) -> dict[str, Any]:
    """发送一条 aibot 指令并等待匹配的响应；忽略期间其他回调。"""
    rid = uuid.uuid4().hex
    _ws_send(ws, {"cmd": cmd, "headers": {"req_id": rid}, "body": body})
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            raw = ws.recv()
        except Exception:
            break
        if not raw:
            continue
        try:
            msg = json.loads(raw)
        except Exception:
            continue
        if msg.get("headers", {}).get("req_id") == rid:
            return msg
    return {}


def _heartbeat(ws, stop_ev):
    while not stop_ev.is_set():
        stop_ev.wait(29)
        if stop_ev.is_set():
            break
        try:
            with _ws_lock:
                ws.ping()
        except Exception:
            break


def build_aibot_markdown(ctx: dict[str, Any]) -> str:
    """拼一条精简 markdown（控制在单聊 4096 字节上限内）。"""
    date: datetime = ctx["date"]
    weekday = "一二三四五六日"[date.weekday()]
    lines = [f"# 📬 今日精选·{date:%m月%d日} 星期{weekday}\n"]

    hot = ctx.get("hot") or {}
    lines.append("## 🔥 热搜")
    n = 0
    for cat, items in hot.items():
        for m in items[:3]:
            lines.append(f"- {m['title']}")
            n += 1
            if n >= 8:
                break
        if n >= 8:
            break

    jokes = ctx.get("jokes") or []
    if jokes:
        lines.append("\n## 😄 冷笑话")
        lines.append(f"- {jokes[0]}")

    media = ctx.get("media") or {}
    movies = media.get("movies") or []
    musics = media.get("musics") or []
    if movies or musics:
        lines.append("\n## 🎬 影音")
        for m in movies[:2]:
            lines.append(f"- 《{m['title']}》· {m['subtitle']}")
        for m in musics[:2]:
            lines.append(f"- 🎵 {m['title']} - {m['subtitle']}")

    if ctx.get("travels"):
        lines.append("\n## 🧳 旅游推荐")
        for t in ctx["travels"][:2]:
            lines.append(f"- **{t['name']}**｜{t.get('season', '')}")

    page = PAGES_URL or "https://4everyl.github.io/daily-push/"
    lines.append(f"\n> 完整版见网页 {page}")
    lines.append("\n> 📰 每日60秒早报请查看下一条图片消息")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 企业微信群机器人（Webhook）主动推送
# ---------------------------------------------------------------------------

def send_image_via_webhook(image_data: bytes) -> bool:
    """通过群机器人 Webhook 发送图片消息。"""
    if not WEBHOOK_URL:
        return False
    try:
        resp = http_post_json(WEBHOOK_URL, image_payload(image_data))
        if resp.get("errcode") == 0:
            log.info("✅ 群机器人图片推送成功")
            return True
        log.error("❌ 群机器人图片推送失败: %s", resp)
    except Exception as exc:
        log.error("群机器人图片发送异常: %s", exc)
    return False


def send_via_webhook(ctx: dict[str, Any]) -> bool:
    """
    通过企业微信群机器人 Webhook 推送（msgtype=markdown）。
    比智能机器人省心：不需要先和用户互动，也不会被 846607 拦截。
    群机器人 Webhook 地址格式：
        https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=<KEY>
    """
    if not WEBHOOK_URL:
        log.error("未配置 WEBHOOK_URL，无法使用群机器人推送")
        return False
    md = build_aibot_markdown(ctx)
    payload = {"msgtype": "markdown", "markdown": {"content": md}}
    try:
        resp = http_post_json(WEBHOOK_URL, payload)
    except Exception as exc:
        log.error("Webhook 请求失败: %s", exc)
        return False
    if resp.get("errcode") == 0:
        log.info("✅ 群机器人推送成功")
        return True
    log.error("❌ 群机器人推送失败: %s", resp)
    return False


def send_via_aibot(ctx: dict[str, Any], image_url: str | None = None) -> bool:
    """
    通过企业微信智能机器人长连接 API 主动推送。
    流程：WebSocket 连接 → aibot_subscribe 订阅 → aibot_send_msg 推送 → 关闭。
    如传入 image_url，推送完 markdown 后继续发送一条图文消息（封面即早报图）。
    """
    if websocket is None:
        log.error("缺少 websocket-client 库，请先 pip install websocket-client")
        return False
    if not (BOT_ID and BOT_SECRET and CHAT_ID):
        log.error("缺少 BOT_ID / BOT_SECRET / CHAT_ID，无法使用智能机器人推送")
        return False

    md = build_aibot_markdown(ctx)
    stop_ev = threading.Event()
    try:
        ws = websocket.create_connection(WS_URL, timeout=30)
    except Exception as exc:
        log.error("WebSocket 连接失败: %s", exc)
        return False

    hb = threading.Thread(target=_heartbeat, args=(ws, stop_ev), daemon=True)
    hb.start()
    try:
        sub = _send_cmd(ws, "aibot_subscribe", {"bot_id": BOT_ID, "secret": BOT_SECRET})
        if sub.get("errcode") != 0:
            log.error("订阅失败: %s", sub)
            return False
        log.info("✅ 智能机器人订阅成功")

        body = {
            "chatid": CHAT_ID,
            "chat_type": CHAT_TYPE,
            "msgtype": "markdown",
            "markdown": {"content": md},
        }
        resp = _send_cmd(ws, "aibot_send_msg", body)
        ok = resp.get("errcode") == 0
        if ok:
            log.info("✅ 智能机器人推送成功")
        else:
            log.error("❌ 智能机器人推送失败: %s", resp)
            return False

        if image_url:
            news_body = {
                "chatid": CHAT_ID,
                "chat_type": CHAT_TYPE,
                **news_payload(
                    title="📰 每日60秒早报",
                    description="点击卡片查看今日新闻详情",
                    url=image_url,
                    picurl=image_url,
                ),
            }
            resp2 = _send_cmd(ws, "aibot_send_msg", news_body, timeout=30)
            if resp2.get("errcode") == 0:
                log.info("✅ 智能机器人图文消息推送成功")
            else:
                log.error("❌ 智能机器人图文消息推送失败: %s", resp2)
        return True
    finally:
        stop_ev.set()
        try:
            ws.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    log.info("=" * 50)
    log.info("MODE=%s | PAGES_URL=%s", MODE, PAGES_URL or "(未设置)")

    ctx: dict[str, Any] = {
        "date": datetime.now(),
        "hot": collect_hot_by_category(),
        "jokes": collect_jokes(10),
        "media": collect_media(),
        "concerts": collect_concerts(5),
        "travels": collect_travel(3),
        "news": get_daily_news(),
    }

    # 下载 uuhb.cn 每日60秒早报图（独立图片消息，不受 viki 限流影响）
    image_data = download_image(UUHB_60S_IMAGE_URL)

    if MODE in ("html", "all"):
        html = render_html(ctx)
        write_html_files(html, ctx["date"])

    if MODE in ("push", "all"):
        if WEBHOOK_URL:
            ok = send_via_webhook(ctx)
            if ok and image_data:
                send_image_via_webhook(image_data)
        elif BOT_ID and BOT_SECRET and CHAT_ID:
            ok = send_via_aibot(ctx, image_url=UUHB_60S_IMAGE_URL if image_data else None)
        else:
            log.error("未配置任何推送方式（WEBHOOK_URL 或 BOT_ID/BOT_SECRET/CHAT_ID）")
            ok = False
        if not ok:
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())

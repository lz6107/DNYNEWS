import os
import re
import time
import html
import random
import sqlite3
import hashlib
import logging
from io import BytesIO
from typing import List, Dict, Optional
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass


# =========================
# 基础配置
# =========================

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL", "").strip()
SOURCE_PAGES_RAW = os.getenv("SOURCE_PAGES", "").strip()

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "120"))
MAX_POSTS_PER_CHECK = int(os.getenv("MAX_POSTS_PER_CHECK", "3"))
SEND_DELAY_MIN = int(os.getenv("SEND_DELAY_MIN", "30"))
SEND_DELAY_MAX = int(os.getenv("SEND_DELAY_MAX", "90"))

FIRST_RUN_SKIP_OLD = os.getenv("FIRST_RUN_SKIP_OLD", "true").lower() == "true"

SEND_IMAGES = os.getenv("SEND_IMAGES", "true").lower() == "true"
MAX_IMAGES_PER_POST = int(os.getenv("MAX_IMAGES_PER_POST", "3"))

ENABLE_CONTACT_REPLACE = os.getenv("ENABLE_CONTACT_REPLACE", "true").lower() == "true"
CONTACT_TEXT = os.getenv("CONTACT_TEXT", "投稿/商务合作：@你的TG号").replace("\\n", "\n").strip()

REQUIRE_NEWS_KEYWORDS = os.getenv("REQUIRE_NEWS_KEYWORDS", "true").lower() == "true"
MIN_TEXT_LENGTH = int(os.getenv("MIN_TEXT_LENGTH", "20"))

APPEND_SOURCE_LINK = os.getenv("APPEND_SOURCE_LINK", "false").lower() == "true"

DB_PATH = os.getenv("DB_PATH", "data/tg_web_scraper.db")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# 命令配置
COMMANDS_ENABLED = os.getenv("COMMANDS_ENABLED", "true").lower() == "true"
ADMIN_USER_IDS_RAW = os.getenv("ADMIN_USER_IDS", "").strip()
COMMAND_CHECK_INTERVAL = int(os.getenv("COMMAND_CHECK_INTERVAL", "5"))

TEST_FETCH_DEFAULT_COUNT = int(os.getenv("TEST_FETCH_DEFAULT_COUNT", "3"))
TEST_FETCH_MAX_COUNT = int(os.getenv("TEST_FETCH_MAX_COUNT", "10"))
TEST_SEND_DELAY = int(os.getenv("TEST_SEND_DELAY", "3"))


# =========================
# 默认关键词
# =========================

DEFAULT_AD_KEYWORDS = [
    "广告", "广告位", "招商", "招租", "推广", "引流", "代理", "总代", "一级代理",
    "充值", "提现", "盘口", "包网", "棋牌", "彩票", "开盘", "开奖", "博彩",
    "会员群", "付费群", "收徒", "赚钱项目", "网赚", "项目合作",
    "担保", "担保交易", "供需", "资源对接", "商务合作", "广告合作",
    "私聊", "联系客服", "联系飞机", "飞机号", "TG客服", "频道导航",
    "包赔", "包杀", "代充", "跑分", "洗米", "U商", "换汇", "代付",
]

DEFAULT_NEWS_KEYWORDS = [
    "突发", "快讯", "消息", "报道", "通报", "宣布", "表示", "指出", "称",
    "警方", "警察", "执法", "抓捕", "逮捕", "拘留", "遣返", "调查", "查处",
    "案件", "事故", "伤亡", "死亡", "受伤", "失踪", "获救", "救援",
    "诈骗", "电诈", "园区", "边境", "偷渡", "绑架", "勒索", "枪击",
    "政府", "法院", "检方", "总统", "部长", "移民局", "海关",
    "柬埔寨", "缅甸", "菲律宾", "泰国", "老挝", "越南", "马来西亚", "新加坡",
    "金边", "西港", "西哈努克", "妙瓦底", "仰光", "曼谷", "马尼拉",
    "东南亚", "当地时间", "今日", "昨日", "近日", "目前", "现场",
]


def split_keywords(value: str, default: List[str]) -> List[str]:
    value = value.strip()
    if not value:
        return default
    parts = re.split(r"[,，\n|]+", value)
    return [x.strip() for x in parts if x.strip()]


AD_KEYWORDS = split_keywords(os.getenv("AD_KEYWORDS", ""), DEFAULT_AD_KEYWORDS)
NEWS_KEYWORDS = split_keywords(os.getenv("NEWS_KEYWORDS", ""), DEFAULT_NEWS_KEYWORDS)


def parse_admin_ids(raw: str) -> set:
    ids = set()
    for item in re.split(r"[,，\s]+", raw):
        item = item.strip()
        if item.isdigit():
            ids.add(int(item))
    return ids


ADMIN_USER_IDS = parse_admin_ids(ADMIN_USER_IDS_RAW)


# =========================
# 日志
# =========================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("tg-web-scraper")


# =========================
# 数据库
# =========================

class DB:
    def __init__(self, path: str):
        self.path = path
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.init_tables()

    def init_tables(self):
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_key TEXT UNIQUE,
            source_name TEXT,
            source_url TEXT,
            message_id INTEGER,
            text_hash TEXT,
            status TEXT,
            created_at INTEGER
        )
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

        self.conn.commit()

    def was_seen(self, post_key: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM posts WHERE post_key = ? LIMIT 1",
            (post_key,)
        ).fetchone()
        return row is not None

    def save_post(self, post: Dict, status: str):
        text_hash = hashlib.sha256((post.get("text") or "").encode("utf-8")).hexdigest()

        self.conn.execute("""
        INSERT OR IGNORE INTO posts
        (post_key, source_name, source_url, message_id, text_hash, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            post["post_key"],
            post["source_name"],
            post["source_url"],
            int(post["message_id"]),
            text_hash,
            status,
            int(time.time())
        ))
        self.conn.commit()

    def get_setting(self, key: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM settings WHERE key = ? LIMIT 1",
            (key,)
        ).fetchone()
        return row[0] if row else None

    def set_setting(self, key: str, value: str):
        self.conn.execute("""
        INSERT INTO settings (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, value))
        self.conn.commit()

    def is_source_initialized(self, source_name: str) -> bool:
        return self.get_setting(f"source_initialized:{source_name}") == "true"

    def set_source_initialized(self, source_name: str):
        self.set_setting(f"source_initialized:{source_name}", "true")


db = DB(DB_PATH)


# =========================
# 工具函数
# =========================

def normalize_source_page(src: str) -> str:
    src = src.strip()
    if not src:
        return ""

    if src.startswith("@"):
        name = src[1:].strip()
        return f"https://t.me/s/{name}"

    if re.fullmatch(r"[A-Za-z0-9_]+", src):
        return f"https://t.me/s/{src}"

    parsed = urlparse(src)

    if "t.me" not in parsed.netloc:
        return src

    parts = [p for p in parsed.path.split("/") if p]

    if len(parts) >= 2 and parts[0] == "s":
        channel = parts[1]
        return f"https://t.me/s/{channel}"

    if len(parts) >= 1:
        channel = parts[0]
        return f"https://t.me/s/{channel}"

    return src


def get_source_name(source_url: str) -> str:
    parsed = urlparse(source_url)
    parts = [p for p in parsed.path.split("/") if p]

    if len(parts) >= 2 and parts[0] == "s":
        return parts[1]

    if parts:
        return parts[-1]

    return source_url


def make_source_pages() -> List[str]:
    if not SOURCE_PAGES_RAW:
        return []

    pages = []
    for item in re.split(r"[,，\n]+", SOURCE_PAGES_RAW):
        url = normalize_source_page(item)
        if url:
            pages.append(url)

    return list(dict.fromkeys(pages))


def keyword_count(text: str, keywords: List[str]) -> int:
    if not text:
        return 0
    lower = text.lower()
    count = 0
    for kw in keywords:
        if kw and kw.lower() in lower:
            count += 1
    return count


def clean_blank_lines(text: str) -> str:
    lines = [line.rstrip() for line in text.splitlines()]
    result = []
    blank_count = 0

    for line in lines:
        if line.strip() == "":
            blank_count += 1
            if blank_count <= 1:
                result.append("")
        else:
            blank_count = 0
            result.append(line)

    return "\n".join(result).strip()


def html_to_plain_text(node) -> str:
    if node is None:
        return ""

    for br in node.find_all("br"):
        br.replace_with("\n")

    text = node.get_text("", strip=False)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = text.replace("\u200b", "")
    text = clean_blank_lines(text)

    return text


def extract_background_image(style: str) -> Optional[str]:
    if not style:
        return None

    match = re.search(r"background-image\s*:\s*url\(['\"]?(.*?)['\"]?\)", style)
    if match:
        return html.unescape(match.group(1)).strip()

    return None


def fetch_page(url: str) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0 Safari/537.36"
        ),
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }

    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.text


def parse_messages(source_url: str, page_html: str) -> List[Dict]:
    soup = BeautifulSoup(page_html, "lxml")
    source_name = get_source_name(source_url)

    result = []

    for msg in soup.select(".tgme_widget_message"):
        data_post = msg.get("data-post", "").strip()

        if not data_post or "/" not in data_post:
            continue

        try:
            data_source, msg_id_str = data_post.rsplit("/", 1)
            message_id = int(msg_id_str)
        except Exception:
            continue

        text_node = msg.select_one(".tgme_widget_message_text")
        text = html_to_plain_text(text_node)

        images = []

        for photo in msg.select(".tgme_widget_message_photo_wrap"):
            style = photo.get("style", "")
            img_url = extract_background_image(style)
            if img_url:
                images.append(img_url)

            img_tag = photo.select_one("img")
            if img_tag:
                src = img_tag.get("src") or img_tag.get("data-src")
                if src:
                    images.append(html.unescape(src).strip())

        images = list(dict.fromkeys([x for x in images if x]))

        post_key = f"{data_source}:{message_id}"
        link = f"https://t.me/{data_source}/{message_id}"

        result.append({
            "post_key": post_key,
            "source_name": source_name,
            "source_url": source_url,
            "message_id": message_id,
            "text": text,
            "images": images,
            "link": link,
        })

    result.sort(key=lambda x: x["message_id"])
    return result


# =========================
# 联系方式清理 / 替换
# =========================

CONTACT_HANDLE_RE = re.compile(
    r"(@[A-Za-z0-9_]{4,}|https?://t\.me/[A-Za-z0-9_/?=+\-]+|t\.me/[A-Za-z0-9_/?=+\-]+)",
    re.IGNORECASE
)

CONTACT_KEYWORD_RE = re.compile(
    r"(投稿|爆料|联系|客服|商务|合作|广告|频道|群组|群聊|咨询|唯一|管理员|飞机|电报|TG|Telegram|微信|VX|QQ|电话|手机)",
    re.IGNORECASE
)


def is_contact_line(line: str) -> bool:
    s = line.strip()
    if not s:
        return False

    if CONTACT_KEYWORD_RE.search(s) and CONTACT_HANDLE_RE.search(s):
        return True

    if CONTACT_KEYWORD_RE.search(s) and re.search(r"(微信|VX|QQ|电话|手机|客服)", s, re.IGNORECASE):
        return True

    if re.fullmatch(r"(@[A-Za-z0-9_]{4,}|https?://t\.me/[A-Za-z0-9_/?=+\-]+|t\.me/[A-Za-z0-9_/?=+\-]+)", s, re.IGNORECASE):
        return True

    if re.search(r"(投稿联系|爆料联系|广告合作|商务合作|联系方式|联系飞机|联系客服)", s, re.IGNORECASE):
        return True

    return False


def remove_original_contacts(text: str) -> str:
    if not text:
        return ""

    lines = text.splitlines()
    kept = []

    for line in lines:
        if is_contact_line(line):
            continue
        kept.append(line)

    cleaned = "\n".join(kept)

    cleaned = re.sub(r"(?i)商务合作[:：]?\s*@?[A-Za-z0-9_]{4,}", "", cleaned)
    cleaned = re.sub(r"(?i)广告合作[:：]?\s*@?[A-Za-z0-9_]{4,}", "", cleaned)
    cleaned = re.sub(r"(?i)投稿[:：]?\s*@?[A-Za-z0-9_]{4,}", "", cleaned)
    cleaned = re.sub(r"(?i)爆料[:：]?\s*@?[A-Za-z0-9_]{4,}", "", cleaned)

    return clean_blank_lines(cleaned)


def build_final_text(cleaned_text: str, post: Dict) -> str:
    final_text = cleaned_text.strip()

    if ENABLE_CONTACT_REPLACE and CONTACT_TEXT:
        final_text = f"{final_text}\n\n{CONTACT_TEXT}"

    if APPEND_SOURCE_LINK:
        final_text = f"{final_text}\n\n来源：{post['link']}"

    return final_text.strip()


# =========================
# 广告过滤 / 新闻判断
# =========================

def looks_like_ad(raw_text: str, cleaned_text: str) -> bool:
    raw_text = raw_text or ""
    cleaned_text = cleaned_text or ""

    ad_hits_cleaned = keyword_count(cleaned_text, AD_KEYWORDS)
    ad_hits_raw = keyword_count(raw_text, AD_KEYWORDS)
    news_hits_cleaned = keyword_count(cleaned_text, NEWS_KEYWORDS)

    if len(cleaned_text.strip()) < MIN_TEXT_LENGTH and ad_hits_raw > 0:
        return True

    if ad_hits_cleaned >= 1 and news_hits_cleaned == 0:
        return True

    if ad_hits_cleaned >= 2 and news_hits_cleaned <= 1:
        return True

    handle_count = len(CONTACT_HANDLE_RE.findall(raw_text))
    if handle_count >= 3 and news_hits_cleaned == 0:
        return True

    strong_ad_patterns = [
        r"广告位招租",
        r"招商代理",
        r"联系客服",
        r"频道导航",
        r"会员群",
        r"包网",
        r"盘口",
        r"博彩",
        r"充值",
        r"代充",
        r"担保交易",
        r"商务合作",
    ]

    for pattern in strong_ad_patterns:
        if re.search(pattern, raw_text, re.IGNORECASE) and news_hits_cleaned == 0:
            return True

    return False


def is_news_message(cleaned_text: str) -> bool:
    if not cleaned_text or len(cleaned_text.strip()) < MIN_TEXT_LENGTH:
        return False

    if not REQUIRE_NEWS_KEYWORDS:
        return True

    news_hits = keyword_count(cleaned_text, NEWS_KEYWORDS)

    if news_hits > 0:
        return True

    return False


def prepare_post_for_send(post: Dict) -> Optional[str]:
    raw_text = post.get("text") or ""

    if not raw_text.strip():
        return None

    cleaned_text = remove_original_contacts(raw_text)

    if looks_like_ad(raw_text, cleaned_text):
        return None

    if not is_news_message(cleaned_text):
        return None

    final_text = build_final_text(cleaned_text, post)

    if not final_text.strip():
        return None

    return final_text


# =========================
# Telegram Bot API
# =========================

def bot_api(method: str, data: Dict, files=None) -> Dict:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN 没有设置")

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"

    resp = requests.post(url, data=data, files=files, timeout=60)

    try:
        payload = resp.json()
    except Exception:
        raise RuntimeError(f"Telegram 返回非 JSON：HTTP {resp.status_code} {resp.text[:300]}")

    if not resp.ok or not payload.get("ok"):
        raise RuntimeError(f"Telegram API 错误：{payload}")

    return payload


def send_admin_message(chat_id: int, text: str):
    try:
        bot_api("sendMessage", {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        })
    except Exception as e:
        logger.warning(f"回复命令失败：{e}")


def split_text(text: str, limit: int = 3900) -> List[str]:
    text = text.strip()
    if len(text) <= limit:
        return [text]

    chunks = []
    current = ""

    paragraphs = text.split("\n\n")

    for p in paragraphs:
        if len(p) > limit:
            if current:
                chunks.append(current.strip())
                current = ""

            for i in range(0, len(p), limit):
                chunks.append(p[i:i + limit])
            continue

        if len(current) + len(p) + 2 <= limit:
            current = f"{current}\n\n{p}" if current else p
        else:
            chunks.append(current.strip())
            current = p

    if current:
        chunks.append(current.strip())

    return chunks


def send_text(text: str):
    chunks = split_text(text)

    for chunk in chunks:
        bot_api("sendMessage", {
            "chat_id": TARGET_CHANNEL,
            "text": chunk,
            "disable_web_page_preview": "true",
        })

        time.sleep(1.5)


def download_image(image_url: str) -> BytesIO:
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    resp = requests.get(image_url, headers=headers, timeout=60)
    resp.raise_for_status()

    bio = BytesIO(resp.content)
    bio.name = "photo.jpg"
    bio.seek(0)
    return bio


def send_photo(image_url: str, caption: Optional[str] = None):
    data = {
        "chat_id": TARGET_CHANNEL,
        "photo": image_url,
    }

    if caption:
        data["caption"] = caption[:1024]

    try:
        bot_api("sendPhoto", data)
        return
    except Exception as e:
        logger.warning(f"直接用图片URL发送失败，尝试下载后上传：{e}")

    image_file = download_image(image_url)

    data = {
        "chat_id": TARGET_CHANNEL,
    }

    if caption:
        data["caption"] = caption[:1024]

    files = {
        "photo": ("photo.jpg", image_file, "image/jpeg")
    }

    bot_api("sendPhoto", data, files=files)


def send_post(text: str, images: List[str]):
    images = images or []

    if SEND_IMAGES and images:
        images = images[:MAX_IMAGES_PER_POST]

        try:
            if len(text) <= 1024:
                send_photo(images[0], caption=text)

                for extra_img in images[1:]:
                    time.sleep(1.5)
                    try:
                        send_photo(extra_img)
                    except Exception as e:
                        logger.warning(f"额外图片发送失败，跳过：{e}")
            else:
                for img in images:
                    try:
                        send_photo(img)
                        time.sleep(1.5)
                    except Exception as e:
                        logger.warning(f"图片发送失败，跳过：{e}")

                send_text(text)

            return

        except Exception as e:
            logger.warning(f"图片消息发送失败，改为纯文字发送：{e}")

    send_text(text)


# =========================
# 正常采集逻辑
# =========================

def process_post(post: Dict) -> bool:
    if db.was_seen(post["post_key"]):
        return False

    raw_text = post.get("text") or ""

    if not raw_text.strip():
        db.save_post(post, "skipped_empty")
        logger.info(f"跳过空消息：{post['post_key']}")
        return False

    cleaned_text = remove_original_contacts(raw_text)

    if looks_like_ad(raw_text, cleaned_text):
        db.save_post(post, "skipped_ad")
        logger.info(f"跳过广告：{post['post_key']}")
        return False

    if not is_news_message(cleaned_text):
        db.save_post(post, "skipped_not_news")
        logger.info(f"跳过非新闻：{post['post_key']}")
        return False

    final_text = build_final_text(cleaned_text, post)

    if not final_text:
        db.save_post(post, "skipped_empty_after_clean")
        return False

    logger.info(f"准备发送：{post['post_key']} | 图片数：{len(post.get('images') or [])}")

    try:
        send_post(final_text, post.get("images") or [])
        db.save_post(post, "sent")
        logger.info(f"发送成功：{post['post_key']}")
        return True

    except Exception as e:
        logger.error(f"发送失败：{post['post_key']} | {e}")
        db.save_post(post, "failed")
        return False


def process_source(source_url: str, remaining_limit: int) -> int:
    source_name = get_source_name(source_url)

    logger.info(f"开始检查源频道：{source_url}")

    try:
        page_html = fetch_page(source_url)
        posts = parse_messages(source_url, page_html)
    except Exception as e:
        logger.error(f"采集失败：{source_url} | {e}")
        return 0

    if not posts:
        logger.warning(f"没有解析到消息：{source_url}")
        return 0

    logger.info(f"解析到 {len(posts)} 条消息：{source_name}")

    if not db.is_source_initialized(source_name):
        if FIRST_RUN_SKIP_OLD:
            for post in posts:
                db.save_post(post, "skipped_initial")
            db.set_source_initialized(source_name)
            logger.info(f"首次运行，已跳过旧消息：{source_name} | 数量：{len(posts)}")
            return 0
        else:
            db.set_source_initialized(source_name)

    sent_count = 0

    for post in posts:
        if sent_count >= remaining_limit:
            break

        if db.was_seen(post["post_key"]):
            continue

        sent = process_post(post)

        if sent:
            sent_count += 1

            delay = random.randint(SEND_DELAY_MIN, SEND_DELAY_MAX)
            logger.info(f"等待 {delay} 秒后继续")
            time.sleep(delay)
        else:
            time.sleep(1)

    return sent_count


# =========================
# 测试命令：立即抓历史并发送
# =========================

def test_fetch_history(count: int, reply_chat_id: int):
    source_pages = make_source_pages()

    if not source_pages:
        send_admin_message(reply_chat_id, "SOURCE_PAGES 没有配置，无法测试。")
        return

    count = max(1, min(count, TEST_FETCH_MAX_COUNT))

    send_admin_message(
        reply_chat_id,
        f"开始测试抓取历史消息，准备从源频道里筛选最新 {count} 条新闻发送到：{TARGET_CHANNEL}"
    )

    candidates = []

    for source_url in source_pages:
        try:
            page_html = fetch_page(source_url)
            posts = parse_messages(source_url, page_html)
            logger.info(f"测试抓取：{source_url} 解析到 {len(posts)} 条")

            for post in posts:
                final_text = prepare_post_for_send(post)
                if not final_text:
                    continue

                candidates.append({
                    "post": post,
                    "final_text": final_text,
                })

        except Exception as e:
            logger.error(f"测试抓取源失败：{source_url} | {e}")
            send_admin_message(reply_chat_id, f"源抓取失败：{source_url}\n错误：{e}")

    if not candidates:
        send_admin_message(reply_chat_id, "没有筛选到可发送的新闻。可能是关键词太严，或者源页面没有新内容。")
        return

    candidates = candidates[-count:]

    sent = 0
    failed = 0

    for item in candidates:
        post = item["post"]
        final_text = item["final_text"]

        try:
            send_post(final_text, post.get("images") or [])
            db.save_post(post, "sent_test")
            sent += 1
            logger.info(f"测试发送成功：{post['post_key']}")
            time.sleep(TEST_SEND_DELAY)

        except Exception as e:
            failed += 1
            logger.error(f"测试发送失败：{post['post_key']} | {e}")

    send_admin_message(
        reply_chat_id,
        f"测试完成。\n成功发送：{sent} 条\n失败：{failed} 条\n目标频道：{TARGET_CHANNEL}"
    )


# =========================
# 命令处理
# =========================

def get_updates(offset: Optional[int] = None) -> Dict:
    data = {
        "timeout": 0,
        "limit": 20,
        "allowed_updates": '["message"]',
    }

    if offset is not None:
        data["offset"] = offset

    return bot_api("getUpdates", data)


def init_command_offset():
    if not COMMANDS_ENABLED:
        return

    if db.get_setting("command_offset"):
        return

    try:
        payload = get_updates()
        updates = payload.get("result", [])

        if updates:
            max_update_id = max(u.get("update_id", 0) for u in updates)
            db.set_setting("command_offset", str(max_update_id + 1))
            logger.info(f"已跳过旧命令 update，offset={max_update_id + 1}")
        else:
            db.set_setting("command_offset", "0")
            logger.info("命令 offset 初始化完成，没有旧命令")

    except Exception as e:
        logger.warning(f"初始化命令 offset 失败：{e}")


def is_admin_user(user_id: int) -> bool:
    return user_id in ADMIN_USER_IDS


def parse_command(text: str) -> str:
    text = text.strip()
    if not text.startswith("/"):
        return ""

    first = text.split()[0]
    first = first.split("@")[0]
    return first.lower()


def handle_command_message(message: Dict):
    chat = message.get("chat", {}) or {}
    from_user = message.get("from", {}) or {}

    chat_id = chat.get("id")
    user_id = from_user.get("id")
    text = message.get("text", "") or ""

    if not chat_id or not user_id or not text:
        return

    cmd = parse_command(text)

    if cmd == "/id":
        send_admin_message(
            chat_id,
            f"你的 Telegram 数字ID是：{user_id}\n\n把它填到 Railway：\nADMIN_USER_IDS={user_id}"
        )
        return

    if cmd in ["/start", "/help"]:
        send_admin_message(
            chat_id,
            "可用命令：\n"
            "/id - 获取你的数字ID\n"
            "/testfetch 3 - 立即抓取历史新闻测试发送\n"
            "/status - 查看基础配置状态"
        )
        return

    if not is_admin_user(user_id):
        send_admin_message(
            chat_id,
            "你还没有管理员权限。\n\n先发送 /id 获取数字ID，然后把它填到 Railway 的 ADMIN_USER_IDS 里。"
        )
        return

    if cmd == "/status":
        source_pages = make_source_pages()
        send_admin_message(
            chat_id,
            "当前状态：\n"
            f"目标频道：{TARGET_CHANNEL}\n"
            f"源频道数量：{len(source_pages)}\n"
            f"发送图片：{SEND_IMAGES}\n"
            f"过滤广告关键词数量：{len(AD_KEYWORDS)}\n"
            f"新闻关键词数量：{len(NEWS_KEYWORDS)}\n"
            f"联系方式替换：{ENABLE_CONTACT_REPLACE}\n"
            f"测试最多发送：{TEST_FETCH_MAX_COUNT} 条"
        )
        return

    if cmd == "/testfetch":
        parts = text.split()
        count = TEST_FETCH_DEFAULT_COUNT

        if len(parts) >= 2 and parts[1].isdigit():
            count = int(parts[1])

        test_fetch_history(count, chat_id)
        return


def handle_commands_once():
    if not COMMANDS_ENABLED:
        return

    offset_raw = db.get_setting("command_offset")
    offset = int(offset_raw) if offset_raw and offset_raw.isdigit() else 0

    try:
        payload = get_updates(offset=offset)
        updates = payload.get("result", [])

        if not updates:
            return

        max_update_id = offset

        for update in updates:
            update_id = update.get("update_id", 0)
            max_update_id = max(max_update_id, update_id + 1)

            message = update.get("message")
            if not message:
                continue

            handle_command_message(message)

        db.set_setting("command_offset", str(max_update_id))

    except Exception as e:
        logger.warning(f"处理命令失败：{e}")


def sleep_with_commands(total_seconds: int):
    elapsed = 0

    while elapsed < total_seconds:
        step = min(COMMAND_CHECK_INTERVAL, total_seconds - elapsed)
        time.sleep(step)
        elapsed += step
        handle_commands_once()


# =========================
# 启动检查 / 主循环
# =========================

def check_required_config():
    errors = []

    if not BOT_TOKEN:
        errors.append("BOT_TOKEN 没填")

    if not TARGET_CHANNEL:
        errors.append("TARGET_CHANNEL 没填")

    if not SOURCE_PAGES_RAW:
        errors.append("SOURCE_PAGES 没填")

    if errors:
        raise RuntimeError("配置错误：" + "；".join(errors))


def main():
    check_required_config()

    source_pages = make_source_pages()

    if not source_pages:
        raise RuntimeError("没有有效的 SOURCE_PAGES")

    logger.info("TG 网页采集机器人启动")
    logger.info(f"目标频道：{TARGET_CHANNEL}")
    logger.info(f"源频道数量：{len(source_pages)}")
    logger.info(f"数据库路径：{DB_PATH}")
    logger.info(f"首次运行跳过旧消息：{FIRST_RUN_SKIP_OLD}")
    logger.info(f"发送图片：{SEND_IMAGES}")
    logger.info(f"要求新闻关键词：{REQUIRE_NEWS_KEYWORDS}")
    logger.info(f"命令功能：{COMMANDS_ENABLED}")
    logger.info(f"管理员数量：{len(ADMIN_USER_IDS)}")

    init_command_offset()

    while True:
        try:
            handle_commands_once()

            sent_this_round = 0

            for source_url in source_pages:
                remaining = MAX_POSTS_PER_CHECK - sent_this_round

                if remaining <= 0:
                    break

                sent = process_source(source_url, remaining)
                sent_this_round += sent

                handle_commands_once()

            logger.info(f"本轮完成，发送 {sent_this_round} 条，等待 {CHECK_INTERVAL} 秒")
            sleep_with_commands(CHECK_INTERVAL)

        except KeyboardInterrupt:
            logger.info("手动停止")
            break

        except Exception as e:
            logger.error(f"主循环错误：{e}")
            sleep_with_commands(30)


if __name__ == "__main__":
    main()

import json
import logging
import os
import re
import secrets
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_FEED_LIST_PATH = Path(__file__).resolve().parent / "feeds.json"
_DEFAULT_FETCH_TIMEOUT_S = 30
_MAX_WORKERS = 16


def _domain_label_without_tld(hostname: str) -> str:
    """Derive a stable folder label (lowercase, no public suffix) from the feed host."""
    host = hostname.lower().strip()
    if not host:
        return "unknown"
    if ":" in host:
        host = host.split(":", 1)[0]
    parts = host.split(".")
    if parts[0] == "www":
        parts = parts[1:]
    if not parts:
        return "unknown"
    # europarl.europa.eu → europarl
    if len(parts) >= 3 and parts[-1] == "eu" and parts[-2] == "europa":
        label = parts[0]
    elif (
        len(parts) >= 3
        and len(parts[-1]) == 2
        and parts[-2] in ("co", "com", "net", "org", "gov", "edu", "ac", "sch")
    ):
        # example.co.uk, example.com.au → leftmost registrable label
        label = parts[-3] if len(parts) >= 3 else parts[0]
    elif len(parts) == 2:
        label = parts[0]
    elif len(parts) == 3:
        # sub.example.com → example; rss.focus.de → focus
        label = parts[1]
    elif len(parts) >= 4 and parts[-1] == "uk" and parts[-2] in (
        "co",
        "gov",
        "ac",
        "org",
        "net",
        "sch",
        "ltd",
        "plc",
    ):
        label = parts[-3]
    else:
        label = parts[-2]
    safe = re.sub(r"[^a-z0-9_-]+", "-", label).strip("-")
    return safe or "unknown"


def _short_id() -> str:
    return secrets.token_hex(2)


def _load_feed_list() -> list[dict[str, Any]]:
    with _FEED_LIST_PATH.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("feeds.json must contain a JSON array")
    return data


def _fetch_feed(url: str, timeout_s: int) -> tuple[int, bytes, str | None]:
    req = Request(
        url,
        headers={
            "User-Agent": "rss-archive-lambda/1.0 (+https://github.com/aws-lambda)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        },
    )
    with urlopen(req, timeout=timeout_s) as resp:
        status = getattr(resp, "status", 200)
        body = resp.read()
        ctype = resp.headers.get("Content-Type")
    return status, body, ctype


def _process_one_feed(
    item: dict[str, Any],
    bucket: str,
    timeout_s: int,
    s3_client: Any,
    date_prefix: str,
) -> dict[str, Any]:
    xml_url = item.get("xmlUrl") or ""
    title = item.get("title")
    parsed = urlparse(xml_url)
    host = parsed.hostname or ""
    domain_folder = _domain_label_without_tld(host)

    if not xml_url or not host:
        return {
            "xmlUrl": xml_url,
            "title": title,
            "ok": False,
            "error": "missing xmlUrl or hostname",
        }

    try:
        status, body, ctype = _fetch_feed(xml_url, timeout_s)
    except HTTPError as e:
        return {
            "xmlUrl": xml_url,
            "title": title,
            "ok": False,
            "error": f"HTTP {e.code}",
        }
    except URLError as e:
        return {
            "xmlUrl": xml_url,
            "title": title,
            "ok": False,
            "error": f"URL error: {e.reason}",
        }
    except Exception as e:  # noqa: BLE001
        return {
            "xmlUrl": xml_url,
            "title": title,
            "ok": False,
            "error": str(e),
        }

    if status != 200 or not body:
        return {
            "xmlUrl": xml_url,
            "title": title,
            "ok": False,
            "error": f"HTTP status {status} or empty body",
        }

    sid = _short_id()
    key = f"feeds/{domain_folder}/{date_prefix}-{sid}.json"
    payload = {
        "title": title,
        "text": item.get("text"),
        "xmlUrl": xml_url,
        "htmlUrl": item.get("htmlUrl"),
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "httpStatus": status,
        "contentType": ctype,
        "bodyText": body.decode("utf-8", errors="replace"),
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )

    return {
        "xmlUrl": xml_url,
        "title": title,
        "ok": True,
        "s3Key": key,
        "bytes": len(body),
    }


def handler(event, context):
    bucket = os.environ.get("FEEDS_S3_BUCKET") or os.environ.get("S3_BUCKET")
    if not bucket:
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": "FEEDS_S3_BUCKET or S3_BUCKET environment variable not set"}
            ),
        }

    timeout_s = int(os.environ.get("FEED_FETCH_TIMEOUT_S", _DEFAULT_FETCH_TIMEOUT_S))
    date_prefix = datetime.now(timezone.utc).strftime("%d-%m-%Y")

    try:
        feed_items = _load_feed_list()
    except Exception as e:  # noqa: BLE001
        logger.exception("Failed to load feeds.json")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }

    s3 = boto3.client("s3")
    results: list[dict[str, Any]] = []
    ok_count = 0

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _process_one_feed,
                item,
                bucket,
                timeout_s,
                s3,
                date_prefix,
            ): item
            for item in feed_items
        }
        for fut in as_completed(futures):
            results.append(fut.result())
            if results[-1].get("ok"):
                ok_count += 1

    failed = [r for r in results if not r.get("ok")]
    summary = {
        "bucket": bucket,
        "total": len(feed_items),
        "stored": ok_count,
        "failed": len(failed),
        "results": results,
    }
    return {
        "statusCode": 200,
        "body": json.dumps(summary, ensure_ascii=False),
    }

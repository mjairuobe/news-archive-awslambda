import asyncio
import json
import logging
import os
import re
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import httpx
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

_FEED_LIST_PATH = Path(__file__).resolve().parent / "feeds.json"
_DEFAULT_FETCH_TIMEOUT_S = 30
_DEFAULT_CONCURRENT_REQUESTS = 5
_MAX_CONCURRENT_CAP = 500
# Default bucket if no env var: globally unique per account + region (S3 naming rules).
_DEFAULT_BUCKET_PREFIX = "rss-news-archive"

_HTTP_HEADERS = {
    "User-Agent": "rss-archive-lambda/1.0 (+https://github.com/aws-lambda)",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


def _aws_region() -> str:
    return (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or "eu-central-1"
    )


def _default_bucket_name(account_id: str, region: str) -> str:
    r = region.replace("_", "-").lower()
    return f"{_DEFAULT_BUCKET_PREFIX}-{account_id}-{r}"


def _resolve_bucket_name() -> str:
    for key in ("FEEDS_S3_BUCKET", "S3_BUCKET"):
        b = os.environ.get(key)
        if b:
            return b.strip()
    sts = boto3.client("sts")
    aid = sts.get_caller_identity()["Account"]
    return _default_bucket_name(aid, _aws_region())


def _ensure_bucket_exists(s3_client: Any, bucket: str, region: str) -> None:
    """Create the bucket in this region if it does not exist."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = e.response["Error"]["Code"]
        # 403: exists but no access, or bucket in other account
        if str(code) in ("403", "AccessDenied"):
            raise RuntimeError(
                f"S3 bucket {bucket!r}: access denied on HeadBucket; "
                "check IAM or set FEEDS_S3_BUCKET / S3_BUCKET to a bucket you control."
            ) from e
        # Missing bucket (SDK/API variants)
        if str(code) not in (
            "404",
            "NotFound",
            "NoSuchBucket",
        ):
            raise RuntimeError(
                f"Cannot verify S3 bucket {bucket!r}: {e.response['Error'].get('Message', code)}"
            ) from e

    params: dict[str, Any] = {"Bucket": bucket}
    if region != "us-east-1":
        params["CreateBucketConfiguration"] = {
            "LocationConstraint": region,
        }
    try:
        s3_client.create_bucket(**params)
        logger.info("Created S3 bucket %s in region %s", bucket, region)
    except ClientError as e:
        create_code = e.response["Error"]["Code"]
        if create_code == "BucketAlreadyOwnedByYou":
            return
        if create_code == "BucketAlreadyExists":
            raise RuntimeError(
                f"S3 bucket name {bucket!r} is already taken globally. "
                "Set FEEDS_S3_BUCKET or S3_BUCKET to an available name."
            ) from e
        if create_code == "OperationAborted":
            try:
                s3_client.head_bucket(Bucket=bucket)
                return
            except ClientError:
                pass
        raise RuntimeError(
            f"Could not create S3 bucket {bucket!r}: {e.response['Error'].get('Message', create_code)}"
        ) from e


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


def _parse_concurrent_requests() -> int:
    raw = os.environ.get(
        "RSS_FEED_CONCURRENT_REQUESTS",
        str(_DEFAULT_CONCURRENT_REQUESTS),
    )
    try:
        n = int(raw.strip())
    except ValueError:
        logger.warning(
            "Invalid RSS_FEED_CONCURRENT_REQUESTS=%r, using default %s",
            raw,
            _DEFAULT_CONCURRENT_REQUESTS,
        )
        return _DEFAULT_CONCURRENT_REQUESTS
    return max(1, min(n, _MAX_CONCURRENT_CAP))


async def _process_one_feed(
    client: httpx.AsyncClient,
    item: dict[str, Any],
    bucket: str,
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
        resp = await client.get(xml_url)
        status = resp.status_code
        body = resp.content
        ctype = resp.headers.get("Content-Type")
    except httpx.RequestError as e:
        return {
            "xmlUrl": xml_url,
            "title": title,
            "ok": False,
            "error": f"request error: {e}",
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
    body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    await asyncio.to_thread(
        s3_client.put_object,
        Bucket=bucket,
        Key=key,
        Body=body_bytes,
        ContentType="application/json; charset=utf-8",
    )

    return {
        "xmlUrl": xml_url,
        "title": title,
        "ok": True,
        "s3Key": key,
        "bytes": len(body),
    }


async def _run_async(
    feed_items: list[dict[str, Any]],
    bucket: str,
    timeout_s: int,
    max_concurrent: int,
    s3_client: Any,
    date_prefix: str,
) -> tuple[list[dict[str, Any]], int]:
    timeout = httpx.Timeout(timeout_s)
    limits = httpx.Limits(max_connections=max(max_concurrent * 2, 16))
    sem = asyncio.Semaphore(max_concurrent)

    async def bounded(item: dict[str, Any], client: httpx.AsyncClient) -> dict[str, Any]:
        async with sem:
            return await _process_one_feed(
                client,
                item,
                bucket,
                s3_client,
                date_prefix,
            )

    async with httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=timeout,
        limits=limits,
        follow_redirects=True,
    ) as client:
        results = await asyncio.gather(
            *(bounded(item, client) for item in feed_items)
        )
    ok_count = sum(1 for r in results if r.get("ok"))
    return list(results), ok_count


def handler(event, context):
    region = _aws_region()
    try:
        bucket = _resolve_bucket_name()
    except Exception as e:  # noqa: BLE001
        logger.exception("Could not resolve S3 bucket name")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    timeout_s = int(os.environ.get("FEED_FETCH_TIMEOUT_S", _DEFAULT_FETCH_TIMEOUT_S))
    max_concurrent = _parse_concurrent_requests()
    date_prefix = datetime.now(timezone.utc).strftime("%d-%m-%Y")

    try:
        feed_items = _load_feed_list()
    except Exception as e:  # noqa: BLE001
        logger.exception("Failed to load feeds.json")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }

    s3 = boto3.client("s3", region_name=region)
    try:
        _ensure_bucket_exists(s3, bucket, region)
    except RuntimeError as e:
        logger.error("%s", e)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    results, ok_count = asyncio.run(
        _run_async(
            feed_items,
            bucket,
            timeout_s,
            max_concurrent,
            s3,
            date_prefix,
        )
    )

    failed = [r for r in results if not r.get("ok")]
    summary = {
        "bucket": bucket,
        "maxConcurrentRequests": max_concurrent,
        "total": len(feed_items),
        "stored": ok_count,
        "failed": len(failed),
        "results": results,
    }
    return {
        "statusCode": 200,
        "body": json.dumps(summary, ensure_ascii=False),
    }

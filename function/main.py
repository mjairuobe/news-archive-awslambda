import feedparser
import json

def handler(event, context):
    """Fetch RSS feed from taz.de as a test."""
    feed_url = "https://www.taz.de/rss/taz-artikel/"
    try:
        feed = feedparser.parse(feed_url)
        entries = []
        for entry in feed.entries[:5]:
            entries.append({
                "title": entry.title,
                "link": entry.link,
                "published": entry.published
            })
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Successfully fetched taz.de RSS feed",
                "entries": entries
            })
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }

import aiohttp
from collections import defaultdict
import asyncio
import random
import logging
from datetime import datetime, timedelta
import hashlib
from typing import AsyncGenerator, Any, Dict, List
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    ExternalId,
    Url,
    Domain,
)
logging.basicConfig(level=logging.INFO)

# Constants
PROXY_LIST = [
    "http://proxy-host-01:3128",
    "http://proxy-host-02:3128",
    "http://proxy-host-03:3128",
    "http://proxy-host-04:3128",
    "http://proxy-host-05:3128",
    # Add more proxies as needed
]

DEFAULT_OLDNESS_SECONDS = 3600
DEFAULT_MAXIMUM_ITEMS = 20
DEFAULT_MIN_POST_LENGTH = 10
DEFAULT_MAX_CONCURRENT_QUERIES = 20

# Initialize a dictionary to keep track of seen post IDs with their timestamps
seen_posts = defaultdict(lambda: datetime.utcnow())

async def fetch_posts(session: aiohttp.ClientSession, keyword: str, since: str, proxy: str) -> list:
    url = f"https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts?q={keyword}&since={since}"
    async with session.get(url, proxy=proxy) as response:
        if response.status == 200:
            data = await response.json()
            return data.get('posts', [])
        else:
            logging.error(f"Failed to fetch posts for keyword {keyword} using proxy {proxy}: {response.status}")
            return []

async def fetch_keyword(session: aiohttp.ClientSession) -> str:
    url = "http://keyword_server:8080/get_keyword"
    async with session.get(url) as response:
        if response.status == 200:
            keyword = await response.text()
            return keyword
        else:
            logging.error(f"Failed to fetch keyword: {response.status}")
            return ""

async def release_keyword(session: aiohttp.ClientSession, keyword: str) -> None:
    url = "http://keyword_server:8080/release_keyword"
    async with session.post(url, json={"keyword": keyword}) as response:
        if response.status != 200:
            logging.error(f"Failed to release keyword: {response.status}")

def calculate_since(max_oldness_seconds: int) -> str:
    since_time = datetime.utcnow() - timedelta(seconds=max_oldness_seconds)
    return since_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

def convert_to_web_url(uri: str, user_handle: str) -> str:
    base_url = "https://bsky.app/profile"
    post_id = uri.split("/")[-1]
    web_url = f"{base_url}/{user_handle}/post/{post_id}"
    return web_url

def format_date_string(date_string: str) -> str:
    try:
        dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        try:
            dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            try:
                dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%f%z")
            except ValueError:
                try:
                    dt = datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S%z")
                except ValueError:
                    raise ValueError(f"Unsupported date format: {date_string}")

    formatted_timestamp = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return formatted_timestamp

def read_parameters(parameters):
    if parameters and isinstance(parameters, dict):
        max_oldness_seconds = parameters.get("max_oldness_seconds", DEFAULT_OLDNESS_SECONDS)
        maximum_items_to_collect = parameters.get("maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS)
        min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
    else:
        max_oldness_seconds = DEFAULT_OLDNESS_SECONDS
        maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return (
        max_oldness_seconds,
        maximum_items_to_collect,
        min_post_length
    )

async def query_single_keyword(
    keyword: str, 
    since: str, 
    proxy: str, 
    max_items: int, 
    min_post_length: int, 
    seen_posts: Dict[str, datetime], 
    max_oldness_seconds: int
) -> List[Item]:
    items = []
    async with aiohttp.ClientSession() as session:
        posts = await fetch_posts(session, keyword, since, proxy)
        current_time = datetime.utcnow()
        
        # Remove expired post IDs from the dictionary
        for post_id in list(seen_posts.keys()):
            if (current_time - seen_posts[post_id]).total_seconds() > max_oldness_seconds:
                del seen_posts[post_id]
        
        for post in posts:
            try:
                if len(items) >= max_items:
                    break

                post_id = post["uri"]
                
                # Skip if post ID is already seen and within the valid time window
                if post_id in seen_posts:
                    continue
                
                datestr = format_date_string(post['record']["createdAt"])
                author_handle = post["author"]["handle"]

                sha1 = hashlib.sha1()
                sha1.update(author_handle.encode())
                author_sha1_hex = sha1.hexdigest()

                url_recomposed = convert_to_web_url(post["uri"], author_handle)
                full_content = post["record"]["text"] + " " + " ".join(
                    image.get("alt", "") for image in post.get("record", {}).get("embed", {}).get("images", [])
                )

                logging.info(f"[Bluesky] Found post: url: %s, date: %s, content: %s", url_recomposed, datestr, full_content)

                item_ = Item(
                    content=Content(str(full_content)),
                    author=Author(str(author_sha1_hex)),
                    created_at=CreatedAt(str(datestr)),
                    domain=Domain("bsky.app"),
                    external_id=ExternalId(post["uri"]),
                    url=Url(url_recomposed),
                )
                
                # Add post ID to seen posts with the current timestamp
                seen_posts[post_id] = current_time
                items.append(item_)

            except Exception as e:
                logging.exception(f"[Bluesky] Error processing post: {e}")

    return items


# Adjust the query function to pass the seen_posts dictionary
async def query(parameters: dict) -> AsyncGenerator[Dict[str, Any], None]:
    max_oldness_seconds, maximum_items_to_collect, min_post_length = read_parameters(parameters)
    max_concurrent_queries = parameters.get("max_concurrent_queries", DEFAULT_MAX_CONCURRENT_QUERIES)

    since = calculate_since(max_oldness_seconds)
    yielded_items = 0

    tasks = []

    seen_posts = defaultdict(lambda: datetime.utcnow())  # Initialize seen_posts dict

    async with aiohttp.ClientSession() as session:
        for i in range(max_concurrent_queries):
            if yielded_items >= maximum_items_to_collect:
                break

            keyword = await fetch_keyword(session)
            if not keyword:
                continue

            proxy = random.choice(PROXY_LIST)
            task = query_single_keyword(
                keyword, 
                since, 
                proxy, 
                maximum_items_to_collect, 
                min_post_length, 
                seen_posts, 
                max_oldness_seconds
            )
            tasks.append((task, keyword))

        for task, keyword in tasks:
            results = await task
            for item in results:
                yield item
                yielded_items += 1
                if yielded_items >= maximum_items_to_collect:
                    break
            if yielded_items >= maximum_items_to_collect:
                break
            
            # Release the keyword after use
            await release_keyword(session, keyword)

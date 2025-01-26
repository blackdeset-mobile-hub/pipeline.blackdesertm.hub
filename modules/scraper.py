import requests
import logging
from bs4 import BeautifulSoup

# Logging 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_page_content(url: str) -> BeautifulSoup:
    """
    Fetches the HTML content of a given URL and returns a BeautifulSoup object.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        response.encoding = "utf-8"
        return BeautifulSoup(response.text, "html.parser")
    except requests.exceptions.RequestException as error:
        logger.error(f"Error fetching URL {url}: {error}")
        return None


def extract_post_header(soup: BeautifulSoup):
    """
    Extracts the post header details such as title, date, and author information.
    """
    header_section = soup.find("div", class_="boardHead")
    if not header_section:
        return None, None, None, None, None, False

    title_element = header_section.find("strong")
    date_element = header_section.find("span", class_="datetime")
    title = title_element.text.strip() if title_element else None
    date = date_element.text.strip() if date_element else None

    admin_element = header_section.find("span", class_="master")
    user_element = header_section.find("span", class_="user")

    if admin_element:
        author_name = admin_element.text.strip()
        return title, date, author_name, None, None, True
    elif user_element:
        author_name = user_element.text.strip()
        author_id = user_element.attrs.get("data-no")
        author_region = user_element.attrs.get("data-region")
        return title, date, author_name, author_id, author_region, False

    return title, date, None, None, None, False


def extract_post_content(soup: BeautifulSoup):
    """
    Extracts the main content of the post.
    """
    content_section = soup.find("div", class_="boardContent")
    return content_section.find("div", class_="textCont").get_text(strip=True) if content_section else None


def extract_post_statistics(soup: BeautifulSoup):
    """
    Extracts likes and views statistics from the post.
    """
    stats_section = soup.find("div", class_="infoArea")
    likes = stats_section.find("a", class_="like").text.strip() if stats_section and stats_section.find("a", class_="like") else None
    views = stats_section.find("span", class_="view").text.strip() if stats_section and stats_section.find("span", class_="view") else None
    return likes, views


def extract_replies(soup: BeautifulSoup, post_id: int):
    """
    Extracts the replies to the post.
    """
    replies = []
    reply_section = soup.find("div", class_="replyWrapper")
    if not reply_section:
        return replies

    reply_items = reply_section.find_all("div", class_="replyItem")
    for reply_item in reply_items:
        user_element = reply_item.find("span", class_="user")
        if not user_element:
            continue

        user_id = user_element.attrs.get("data-no") if user_element else None
        user_region = user_element.attrs.get("data-region") if user_element else None
        user_name = user_element.text.strip() if user_element else None

        date_element = reply_item.find("span", class_="datetime")
        likes_element = reply_item.find("a", class_="like")
        text_element = reply_item.find("div", class_="replyText")

        replies.append({
            "post_id": post_id,
            "created_at": date_element.text.strip() if date_element else None,
            "user_id": user_id,
            "user_region": user_region,
            "user_name": user_name,
            "text": text_element.get_text(strip=True) if text_element else None,
            "likes": likes_element.text.strip() if likes_element else None,
        })
    return replies


def fetch_blackdesert_post(content_no: int) -> dict:
    """
    Scrapes a Black Desert forum post using its content Number.
    Returns a dictionary containing post details or None if the post is closed.
    """
    url = f"https://forum.blackdesertm.com/Board/Detail?boardNo=1&contentNo={content_no}"
    soup = fetch_page_content(url)

    if not soup:
        return None

    title, date, author_name, author_id, author_region, is_admin = extract_post_header(soup)

    if not title:
        logger.info(f"Post {content_no} is closed or does not exist.")
        return None

    content = extract_post_content(soup)
    likes, views = extract_post_statistics(soup)
    replies = extract_replies(soup, content_no)

    logger.info(f"Successfully scraped post {content_no}")

    return {
        "post_id": content_no,
        "title": title,
        "created_at": date,
        "author_id": author_id,
        "author_region": author_region,
        "author_name": author_name,
        "is_admin": is_admin,
        "content": content,
        "likes": likes,
        "views": views,
        "replies": replies
    }

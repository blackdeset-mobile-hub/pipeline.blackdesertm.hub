import requests
import logging
from bs4 import BeautifulSoup

# Logging 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_blackdesert_post(content_no: int) -> dict:
    """
    Scrapes a Black Desert forum post using its content number.
    Returns a dictionary containing post details or None if the post is closed.
    """
    url = f"https://forum.blackdesertm.com/Board/Detail?boardNo=1&contentNo={content_no}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        response.encoding = "utf-8"
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        post_header = soup.find("div", class_="boardHead")
        if not post_header:
            logger.info(f"Post {content_no} is closed or does not exist.")
            return None

        title = post_header.find("strong")
        date = post_header.find("span", class_="datetime")
        title = title.text.strip() if title else None
        date = date.text.strip() if date else None

        admin_info = post_header.find("span", class_="master")
        user_info = post_header.find("span", class_="user")
        
        if admin_info: 
            author_full_name = admin_info.text.strip()
            author_user_id, author_user_region, author_is_admin = None, None, True
        elif user_info: 
            author_full_name = user_info.text.strip()
            author_user_id = user_info.attrs.get("data-no")
            author_user_region = user_info.attrs.get("data-region")
            author_is_admin = False
        else:
            author_full_name, author_user_id, author_user_region, author_is_admin = None, None, None, False

        post_content_section = soup.find("div", class_="boardContent")
        content = post_content_section.find("div", class_="textCont").get_text(strip=True) if post_content_section else None

        post_info_section = soup.find("div", class_="infoArea")
        likes = post_info_section.find("a", class_="like").text.strip() if post_info_section and post_info_section.find("a", class_="like") else None
        views = post_info_section.find("span", class_="view").text.strip() if post_info_section and post_info_section.find("span", class_="view") else None

        logger.info(f"Successfully scraped post {content_no}")

        return {
            "post_id": content_no,
            "title": title,
            "created_at": date,
            "author_user_id": author_user_id,
            "author_user_region": author_user_region,
            "author_full_name": author_full_name,
            "author_is_admin": author_is_admin,
            "content": content,
            "likes": likes,
            "views": views,
        }

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching post {content_no}: {e}")
        return None

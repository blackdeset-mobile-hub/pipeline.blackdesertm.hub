import requests
import logging
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_blackdesert_post(content_no: int) -> dict:
    """
    Scrape a Black Desert forum post using its content number.
    
    Checks if the post is closed. If not, returns a dictionary with the title,
    date, author, and content of the post.
    """
    url = f'https://forum.blackdesertm.com/Board/Detail?boardNo=1&contentNo={content_no}'

    try:
        response = requests.get(url)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        soup = BeautifulSoup(response.text, 'html.parser')

        board_head = soup.find('div', class_='boardHead')
        if board_head:
            title = board_head.find('strong').text.strip() if board_head.find('strong') else None
            date = board_head.find('span', class_='datetime').text.strip() if board_head.find('span', class_='datetime') else None
            author = board_head.find('span', class_='master').text.strip() if board_head.find('span', class_='master') else None
            
            board_content = soup.find('div', class_='boardContent')
            text_content = board_content.find('div', class_='textCont')
            content = text_content.get_text(strip=True) if text_content else None
    
            logger.info(f"Successfully scraped post {content_no} - {title}")
            
            return {
                'no': content_no,
                'title': title,
                'date': date,
                'author': author,
                'content': content
            }
        else:
            logger.info(f"Post {content_no} is closed.")
            return None
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching post {content_no}: {e}")
        return None

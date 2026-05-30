import logging
import os

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()   

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 

logger = logging.getLogger(__name__)

BASEURL = "alpha-vantage.p.rapidapi.com"

url = f"https://{BASEURL}/query"

api_key = os.getenv("API_KEY")

headers = {
	"x-rapidapi-key": api_key,
	"x-rapidapi-host": BASEURL,
	"Content-Type": "application/json"
}
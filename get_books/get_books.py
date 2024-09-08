import os
import requests
# from bs4 import BeautifulSoup

# Base URL for Gutenberg books in text format
BASE_URL = "https://www.gutenberg.org/"

# Directory to save books
SAVE_DIR = "/Users/srinathsureshkumar/Workspace/map-reduce/gutenberg_books"
os.makedirs(SAVE_DIR, exist_ok=True)

def download_book(book_id):
    try:
        # Construct the URL for the plain text file
        text_url = f"{BASE_URL}/files/{book_id}/{book_id}.txt"
        
        # Send a request to download the book
        response = requests.get(text_url)
        if response.status_code == 200:
            # Save the book content in a text file
            file_path = os.path.join(SAVE_DIR, f"{book_id}.txt")
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            print(f"Downloaded: Book ID {book_id}")
        else:
            print(f"Failed to download Book ID {book_id} (Status: {response.status_code})")
    except Exception as e:
        print(f"Error downloading book {book_id}: {e}")

def download_books(start_id, count):
    downloaded = 0
    book_id = start_id

    while downloaded < count:
        download_book(book_id)
        downloaded += 1
        book_id += 1

# Example to download 5000 books starting from book ID 1
download_books(6858, 10000)

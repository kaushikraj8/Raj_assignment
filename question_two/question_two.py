import requests
import pandas as pd
from bs4 import BeautifulSoup

# Fetch data from the URL
url = 'https://store-directory-api.afterpay.com/api/v1/categories'
response = requests.get(url)
data = response.json()

# Function to clean HTML content
def clean_html(raw_html):
    return BeautifulSoup(raw_html, "html.parser").text

# Extract data and clean HTML content
cleaned_data = []
for item in data['data']:
    attributes = item['attributes']
    cleaned_item = {
        'id': item['id'],
        'name': attributes.get('name', ''),
        'slug': attributes.get('slug', ''),
        'parent_id': attributes.get('parent_id', ''),
        'meta_title': attributes.get('meta_title', ''),
        'meta_description': attributes.get('meta_description', ''),
        'page_title': attributes.get('page_title', ''),
        'page_description': clean_html(attributes.get('page_description', '')),
        'highlighted': attributes.get('highlighted', False),
        'position': attributes.get('position', 0),
        'pinned': attributes.get('pinned', False),
        'sponsored': attributes.get('sponsored', False),
        'published': attributes.get('published', False),
        'published_by_cash': attributes.get('published_by_cash', False),
        'banner_image_url': attributes.get('banner_image_url', ''),
        'is_collection': attributes.get('is_collection', False),
    }
    cleaned_data.append(cleaned_item)

# Convert to DataFrame
df = pd.DataFrame(cleaned_data)
df['page_description'] = df['page_description'].str.strip()
# Save cleaned data to CSV
try:
    df.to_csv('cleaned_data.csv', index=False)
except:
    print("Failed to save the cleaned data to CSV.")

# Filter only parent IDs (parent_id is None)
parent_df = df[df['parent_id'].isnull()]

# Save parent IDs data to CSV
try:
    parent_df.to_csv('parent_ids.csv', index=False)
except:
    print("Failed to save the parent IDs data to CSV.")
else:
    print("Data processing completed successfully.")
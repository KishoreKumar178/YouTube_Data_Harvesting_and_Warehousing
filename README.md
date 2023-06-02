# YouTube Data Harvesting and Warehousing

This is a Python script that scrapes YouTube channel data, including channel statistics, playlists, videos, and comments, using the YouTube Data API. The script allows you to retrieve and store data in both MongoDB and MySQL databases.

## Prerequisites

Before running the script, make sure you have the following:

- Python 3.x installed
- Google API credentials and API key for the YouTube Data API
- Required Python packages installed: googleapiclient, pandas, pymongo, mysql-connector-python, pymysql, datetime, streamlit, sqlalchemy

## Setup

1. Clone the repository or download the script files.
2. Install the required Python packages by running the following command:
   ```
   pip install -r requirements.txt
   ```
3. Replace the `apikey` variable with your YouTube Data API key.
4. Set up the MongoDB and MySQL databases for storing the scraped data.
5. Adjust the database connection details in the code to match your setup (e.g., host, username, password, database name).
6. Run the script using the following command:
   ```
   streamlit run youtube data.py
   ```
7. Access the web application by opening the provided local URL in your web browser.
8. Enter the YouTube channel ID you want to scrape and click the "Scrape Data" button.
9. The script will scrape the channel data, including playlists, videos, and comments, and store it in the MongoDB and MySQL databases.
10. You can view and analyze the scraped data using the web application.

## Functionality

The script provides the following functionality:

- Retrieves channel statistics, including subscriber count, total views, and video count.
- Retrieves playlists for a given YouTube channel.
- Retrieves videos within each playlist, including video details, such as title, description, published date, view count, like count, favorite count, comment count, duration, and thumbnail.
- Retrieves comments for each video, including comment text, author, and publish date.
- Stores the scraped data in both MongoDB and MySQL databases.
- Provides a Streamlit web application for easily entering the YouTube channel ID and viewing the scraped data.

## Data Storage

The script stores the scraped data in two databases:

- **MongoDB**: Stores the channel data, playlists, videos, and comments in separate collections within the database. Each document represents a specific channel, video, or comment.
- **MySQL**: Stores the channel, playlist, video, and comment data in separate tables within the database. Each row represents a specific channel, video, or comment.

## Contributing

Contributions to this project are welcome. Feel free to open issues or submit pull requests for any improvements or bug fixes.


## Acknowledgements

- The script uses the YouTube Data API to retrieve data from YouTube.
- The script relies on various Python packages for data processing, database connections, and web application development.

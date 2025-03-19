import psycopg2
import pandas as pd
from nltk.corpus import stopwords
from nltk import download
from collections import Counter
import matplotlib.pyplot as plt
from wordcloud import WordCloud

# Ensure necessary NLTK data files are downloaded
download('stopwords')
download('punkt')

# PostgreSQL Configuration
db_config = {
    "dbname": "bbc_news",
    "user": "snigdhagogineni",
    "password": "your_password",  # Replace with your PostgreSQL password
    "host": "localhost",
    "port": 5432
}

# Step 1: Fetch Data from PostgreSQL
try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_config)
    query = "SELECT * FROM rss_feed;"
    data = pd.read_sql(query, conn)
    conn.close()
    print("Data fetched successfully!")
except Exception as e:
    print(f"Error: {e}")
    exit()

# Step 2: Data Cleaning
print("Cleaning data...")
# Drop missing values
data.dropna(inplace=True)

# Convert publication_date to datetime
data['publication_date'] = pd.to_datetime(data['publication_date'])

# Remove duplicate headlines
data.drop_duplicates(subset=['headline'], inplace=True)

print("Data after cleaning:")
print(data.head())

# Step 3: Data Analysis
print("Analyzing data...")

# 1. Articles per day
articles_per_day = data.groupby(data['publication_date'].dt.date).size()
print("Articles per day:")
print(articles_per_day)

# 2. Tokenization using a simple custom tokenizer
stop_words = set(stopwords.words('english'))
all_words = ' '.join(data['headline']).lower()

def simple_tokenizer(text):
    return text.split()

tokens = simple_tokenizer(all_words)

# Filter out stop words and non-alphanumeric tokens
filtered_words = [word for word in tokens if word.isalnum() and word not in stop_words]
word_counts = Counter(filtered_words)
print("Top 10 most frequent words:")
print(word_counts.most_common(10))

# Step 3a: Visualization
print("Visualizing data...")

# Plot articles per day
articles_per_day.plot(kind='bar', figsize=(10, 5), color='skyblue')
plt.title('Number of Articles Per Day')
plt.xlabel('Date')
plt.ylabel('Number of Articles')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Plot word frequency as a word cloud
wordcloud = WordCloud(width=800, height=400, background_color='white').generate(' '.join(filtered_words))
plt.figure(figsize=(10, 5))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.title('Most Frequent Words in Headlines')
plt.show()

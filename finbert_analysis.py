import pandas as pd
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer
import numpy as np
from tqdm import tqdm

# Check for GPU availability
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
print(f"Using device: {device}")

# Load the FinBERT model and tokenizer
tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
model = model.to(device)  # Move model to GPU if available

# Read the dataframe that was shown in the image
# Try to read the combined file if it exists, otherwise read the raw data files
try:
    df = pd.read_csv('data_news/combined_headlines.csv')
except FileNotFoundError:
    print("Error: combined_headlines.csv not found")
    exit()

# Function to get FinBERT sentiment scores
def get_finbert_sentiment(text):
    inputs = tokenizer(text, return_tensors="pt", padding=True, truncation=True, max_length=512)
    # Move inputs to the same device as the model
    inputs = {k: v.to(device) for k, v in inputs.items()}
    
    with torch.no_grad():
        outputs = model(**inputs)
    
    scores = torch.nn.functional.softmax(outputs.logits, dim=1)
    scores_np = scores.cpu().numpy()[0]  # Move back to CPU before converting to numpy
    
    # Return scores as 3 decimal places
    return {
        'positive': round(float(scores_np[0]), 3),
        'negative': round(float(scores_np[1]), 3),
        'neutral': round(float(scores_np[2]), 3)
    }

# Apply FinBERT to each headline
sentiment_scores = []
for headline in tqdm(df['Headlines'], desc="Processing headlines"):
    sentiment_scores.append(get_finbert_sentiment(headline))

# Convert results to DataFrame columns
df['positive_score'] = [score['positive'] for score in sentiment_scores]
df['negative_score'] = [score['negative'] for score in sentiment_scores]
df['neutral_score'] = [score['neutral'] for score in sentiment_scores]

# Display the results
print(df[['Headlines', 'positive_score', 'negative_score', 'neutral_score']].head())

# Save the results
df.to_csv('headlines_with_sentiment.csv', index=False) 
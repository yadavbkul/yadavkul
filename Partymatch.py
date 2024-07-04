import pandas as pd
from rapidfuzz import fuzz

def fuzzy_match_dataframe(file_path, min_score=80):
  """
  Performs fuzzy matching on a million-row file using Pandas DataFrames.

  Args:
      file_path (str): Path to the file containing data for matching.
      min_score (int, optional): Minimum score for a match (higher = stricter). Defaults to 80.

  Returns:
      pandas.DataFrame: DataFrame containing matching rows with indices, score, and matched row content.
  """
  # Read data into a DataFrame
  df = pd.read_csv(file_path)

  # Add a column for cleaned data (optional)
  df['clean_data'] = df['your_column_name'].str.strip()  # Replace 'your_column_name' with your actual column

  # Define a function for fuzzy matching (vectorized)
  def match_row(row):
    matches = []
    for compare_row in df.itertuples(index=False):
      score = fuzz.ratio(row['clean_data'], compare_row['clean_data'])
      if score >= min_score:
        matches.append((row.Index, compare_row.Index, score))
    return matches

  # Apply the matching function to each row with progress bar (optional)
  matches_list = df.progress_apply(match_row, axis=1)  # .progress_apply for progress bar

  # Flatten the list of lists into a single DataFrame
  matches_df = pd.DataFrame.explode(matches_list).rename(columns={0: 'source_index', 1: 'match_index', 2: 'score'})

  return matches_df

# Example usage
file_path = "your_data_file.csv"
min_score = 85  # Adjust minimum score as needed

matches_df = fuzzy_match_dataframe(file_path, min_score)

if not matches_df.empty:
  print("Found matches:")
  print(matches_df[['source_index', 'match_index', 'score']].to_string(index=False))
else:
  print("No matches found based on the specified minimum score.")  


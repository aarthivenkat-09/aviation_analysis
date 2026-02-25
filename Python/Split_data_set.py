import pandas as pd
import os

# ====== INPUT & OUTPUT PATHS ======
input_file = r"C:\Users\DELL\OneDrive\Documents\GUVI\Projects\Final Project\Final Project\Master_Dataset\flights.csv"

output_dir = r"C:\Users\DELL\OneDrive\Documents\GUVI\Projects\Final Project\Final Project\Splited Dataset"

# Create output folder if not exists
os.makedirs(output_dir, exist_ok=True)

# ====== READ CSV SAFELY ======
df = pd.read_csv(
    input_file,
    dtype=str,        # prevent 2354.0 / type issues
    na_filter=False   # keep blanks as blanks
)

# ====== SPLIT MONTH-WISE ======
for month, month_df in df.groupby("MONTH"):
    month_df.to_csv(
        os.path.join(output_dir, f"flights_{int(month):02d}.csv"),
        index=False
    )

print("✅ Dataset successfully split into 12 monthly CSV files")


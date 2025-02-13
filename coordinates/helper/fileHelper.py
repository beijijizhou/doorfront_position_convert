import pandas as pd


def save_corrected_data(corrected_data, filename="corrected_doorfront_data"):
    if corrected_data:
        corrected_df = pd.DataFrame(corrected_data)
        corrected_df.to_csv(f"{filename}.csv", index=False)
        print(f"Saved corrected data as {filename}.csv")
        return corrected_df
    return None


def get_random_sample(data):
    size = 0.005
    sample_size = int(len(data) * size)
    return data.sample(n=sample_size, random_state=42)

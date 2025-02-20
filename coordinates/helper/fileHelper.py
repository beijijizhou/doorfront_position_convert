import pandas as pd


def save_corrected_data(corrected_data, filename="corrected_doorfront_data"):
    if corrected_data:
        corrected_df = pd.DataFrame(corrected_data)
        corrected_df.to_csv(f"{filename}.csv", index=False)
        print(f"Saved corrected data as {filename}.csv")
        return corrected_df
    return None


def get_random_sample(data: pd.DataFrame) -> pd.DataFrame:
    size = 0.01
    sample_size = int(len(data) * size)
    return data.sample(n=sample_size, random_state=4)


def read_data(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path)[:100]
    # df = get_random_sample(df)
    print("current data size", len(df))
    return df

def save_to_csv(df: pd.DataFrame, output_file: str) -> None:
    df.to_csv(output_file, index=False)
    print(f"Data saved to {output_file}")

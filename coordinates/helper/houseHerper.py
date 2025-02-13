import pandas as pd
import re

def extract_house_number(address):
    """
    Extracts the house number from an address string.
    Returns None if no valid house number is found.
    """
    match = re.search(r'\b\d+\b', str(address))  # Find first number in the address
    return match.group() if match else None

def compare_house_numbers(file_path):
    """
    Reads the CSV file and compares house numbers from 'address' and 'google_address'.
    Prints mismatched cases and returns statistics on matches/mismatches.

    :param file_path: Path to the CSV file (updated_coordinates.csv)
    :return: Dictionary with match/mismatch counts
    """
    df = pd.read_csv(file_path)

    # Ensure the required columns exist
    if 'address' not in df.columns or 'google_address' not in df.columns:
        raise ValueError("CSV file must contain 'address' and 'google_address' columns.")

    total_rows = len(df)
    match_count = 0
    mismatch_count = 0

    for index, row in df.iterrows():
        original_number = extract_house_number(row["address"])
        google_number = extract_house_number(row["google_address"])

        if original_number == google_number:
            match_count += 1
        else:
            mismatch_count += 1
            print(f"🔴 MISMATCH FOUND:")
            print(f" - Original: {row['address']}")
            print(f" - Google:   {row['google_address']}\n")

    print(f"\n✅ Total Rows: {total_rows}")
    print(f"✅ Matches: {match_count}")
    print(f"❌ Mismatches: {mismatch_count}")

    return {
        "total": total_rows,
        "matches": match_count,
        "mismatches": mismatch_count
    }






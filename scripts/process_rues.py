"""
Script to process RUES data.
"""

from innpulsa.processing.rues import read_and_process_rues


def main():
    """Main function to process RUES data."""
    rues_df = read_and_process_rues()
    print("\nFirst few rows of RUES data:")
    print(rues_df.head())
    print("\nDataset shape:", rues_df.shape)
    print("\nProcessed RUES data saved to 'data/processed/rues_total.csv'.")


if __name__ == "__main__":
    main()

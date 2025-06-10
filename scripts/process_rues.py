"""
Script to process RUES data.
"""

from innpulsa.processing.rues import read_rues


def main():
    """Main function to process RUES data."""
    rues_df = read_rues()
    print("\nFirst few rows of RUES data:")
    print(rues_df.head())
    print("\nDataset shape:", rues_df.shape)


if __name__ == "__main__":
    main()

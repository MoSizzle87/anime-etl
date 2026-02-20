"""
Quick manual test for extract.py functions.
Run: python test_extract_manual.py
"""
from src.config import load_config
from src.extract import extract_kaggle_csv

def test_kaggle_csv():
    """Test Kaggle CSV extraction."""
    print("Testing extract_kaggle_csv()...")
    
    # Load config
    config = load_config()
    csv_path = f"{config['data_raw_path']}/anime.csv"
    
    try:
        # Extract CSV
        df = extract_kaggle_csv(csv_path)
        
        # Verify
        print(f"✅ CSV loaded successfully")
        print(f"   - Shape: {df.shape}")
        print(f"   - Columns: {list(df.columns)}")
        print(f"\nFirst 3 rows:")
        print(df.head(3))
        
    except FileNotFoundError:
        print(f"❌ CSV file not found at: {csv_path}")
        print("   Please download the Kaggle dataset first:")
        print("   kaggle datasets download -d CooperUnion/anime-recommendations-database")
        print("   unzip anime-recommendations-database.zip -d data/raw/")
        
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    test_kaggle_csv()

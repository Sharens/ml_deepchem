import polars as pl
import httpx
import sys
import time

def get_chembl_data(batch_size=1000):
    base_url = r"https://www.ebi.ac.uk/chembl/api/data/activity.json"
    all_activities = []
    offset = 0
    total_count = 0

    with httpx.Client(timeout=120.0) as client:
        while True:
            query_params = {
                "target_organism":"Homo sapiens", 
                "limit": batch_size,
                "offset": offset,
                "format": "json"
            }

            try:
                response = httpx.get(base_url, params=query_params, timeout=120.0)
                response.raise_for_status()
                data = response.json()

                page_meta = data.get('page_meta', {})
                if total_count == 0:
                    total_count = page_meta.get('total_count', 0)
                    print(f"INFO: Downloaded {total_count} records")
                
                activities = data.get('activities', [])
                if not activities:
                    break

                all_activities.extend(activities)

                print(f"INFO: Downloaded batch: {offset} - {offset + len(activities)} / {total_count}")
                offset += batch_size

                if offset >= total_count:
                    break
                
                time.sleep(0.5)
    
            except Exception as e:
                sys.exit(f"ERROR: Failed to retrieve data -- {e}")
                break

    return all_activities
    
if __name__ == "__main__":
    records = get_chembl_data(batch_size=500000)
    
    if records:
        print(f"\n--- Sukces! Pobrano łącznie {len(records)} rekordów. ---")
        
        # Tworzenie DataFrame z pełnych danych
        # Polars świetnie radzi sobie z dużymi listami słowników
        df = pl.DataFrame(records)
        
        print(f"Wymiary ramki danych: {df.shape}")
        print(df.select(['molecule_chembl_id', 'value', 'units', 'type']).head())
        
        # Opcjonalnie: Zapis do pliku Parquet (dużo szybszy i lżejszy niż CSV)
        df.write_parquet("chembl_raw_data.parquet")
    else:
        print("Nie pobrano żadnych danych.")
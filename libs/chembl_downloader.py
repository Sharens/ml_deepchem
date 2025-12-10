import polars as pl
import httpx
import sys

def get_chembl_data():
    base_url = r"https://www.ebi.ac.uk/chembl/api/data/activity.json"
    query_params = {
        "target_organism":"Homo sapiens", 
        "limit": 100,
        "format": "json"
    }

    try:
        response = httpx.get(base_url, params=query_params, timeout=120.0)
        response.raise_for_status()
        data = response.json()
        
        return data
    
    except Exception as e:
        sys.exit(f"ERROR: Failed to retrieve data -- {e}")
        return(None)
    
if __name__ == "__main__":
    result = get_chembl_data()
    if result:
        print(f"Pobrano {len(result.get('activities', []))} rekordów.")
        df = pl.DataFrame(list(result['activities']))
        print(df)
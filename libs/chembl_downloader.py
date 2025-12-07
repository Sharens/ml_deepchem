from chembl_webresource_client.new_client import new_client
from concurrent.futures import ThreadPoolExecutor, as_completed

# Inicjalizacja klienta i QuerySetu (jednorazowa)
chembl_activity = new_client.activity

def fetch_data_batch_final(start_index, batch_size):
    """
    Pobiera pojedynczą paczkę danych używając cięcia (slicing) i konwertuje ją na listę.
    
    Args:
        start_index (int): Początkowy indeks (offset).
        batch_size (int): Liczba rekordów w paczce (limit).
    """
    end_index = start_index + batch_size
    print(f"Pobieranie: indeks {start_index} do {end_index}...")
    try:
        # **Kluczowa i finalna poprawka:**
        # Używamy cięcia QuerySetu i opakowujemy go funkcją list()
        # To wymusza ewaluację QuerySetu i wykonanie zapytania HTTP.
        query_slice = chembl_activity[start_index:end_index]
        batch_data = list(query_slice) 
        
        return batch_data
    except Exception as e:
        print(f"Błąd podczas pobierania paczki (start={start_index}): {e}")
        return []

def get_chembl_data_concurrent(total_records_to_fetch=10000, batch_size=1000, max_workers=10):
    """
    Pobiera dane aktywności ChEMBL równolegle (batchowo) przy użyciu ThreadPoolExecutor.
    """
    
    start_indices = range(0, total_records_to_fetch, batch_size)
    all_results = []
    
    print(f"Przygotowanie do pobrania {len(start_indices)} paczek (po {batch_size} każda) przy użyciu {max_workers} wątków.")
    
    # 2. Wykonanie równoległe
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(fetch_data_batch_final, start_index, batch_size): start_index
            for start_index in start_indices
        }

        # 3. Zbieranie wyników po ich ukończeniu
        for future in as_completed(future_to_index):
            start_index = future_to_index[future]
            try:
                data = future.result()
                all_results.extend(data)
                print(f"Zakończono pobieranie paczki od indeksu {start_index}. Pobranych rekordów: {len(data)}.")
            except Exception as exc:
                print(f'Paczka od indeksu {start_index} wygenerowała wyjątek: {exc}')
                
    print(f"\n--- Zakończono! Łącznie pobrano {len(all_results)} rekordów. ---")
    return all_results

# --- PRZYKŁAD UŻYCIA ---
# Użyj tych samych parametrów, które były w logu:
# pobrane_dane = get_chembl_data_concurrent_final(total_records_to_fetch=10000, batch_size=1000, max_workers=10)
# print(f"Liczba pobranych rekordów: {len(pobrane_dane)}")
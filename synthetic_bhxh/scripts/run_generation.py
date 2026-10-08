import argparse
import os
import shutil
import time
import yaml
import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.data_generator import get_master_counts, generate_chunk, generate_units_table

def check_free_space(min_gb=100):
    total, used, free = shutil.disk_usage('.')
    free_gb = free / (1024**3)
    if free_gb < min_gb:
        print(f"ERROR: Free space is {free_gb:.2f} GB, which is below the threshold of {min_gb} GB.")
        sys.exit(1)
    return free_gb

def save_chunk(df, table_name, scale_dir, chunk_idx, fmt="parquet"):
    out_dir = Path(scale_dir) / table_name
    out_dir.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        out_path = out_dir / f"part-{chunk_idx:05d}.parquet"
        df.to_parquet(out_path, index=False)
    elif fmt == "csv":
        out_path = out_dir / f"part-{chunk_idx:05d}.csv"
        df.to_csv(out_path, index=False, encoding='utf-8')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--scale', type=str, required=True, choices=['100K', '1M', '10M', '100M'])
    parser.add_argument('--csv', action='store_true', help="Generate CSV along with Parquet")
    args = parser.parse_args()

    scale_masters = {'100K': 8333, '1M': 83333, '10M': 833333, '100M': 8333333}
    target_masters = scale_masters[args.scale]

    print(f"Checking free space...")
    free_gb = check_free_space(100 if args.scale == '100M' else 5)
    print(f"Free space: {free_gb:.2f} GB. OK.")
    
    with open("synthetic_bhxh/config/params.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    global_seed = config['generation']['global_seed']

    print(f"Calculating master distribution for {target_masters} masters (seed: {global_seed})...")
    start_time = time.time()
    lengths = get_master_counts(target_masters, global_seed)
    
    # Generate units dataframe
    num_units = config['generation']['num_units'][args.scale]
    df_units = generate_units_table(num_units, global_seed)
    
    print(f"Total masters: {target_masters}, Total details: {lengths.sum()}")
    
    scale_dir = f"synthetic_bhxh/output/{args.scale}"
    Path(scale_dir).mkdir(parents=True, exist_ok=True)
    ans_key_dir = Path("synthetic_bhxh/_answer_key")
    ans_key_dir.mkdir(parents=True, exist_ok=True)
    
    chunk_size = 50000 
    num_chunks = (target_masters + chunk_size - 1) // chunk_size
    
    gen_csv = args.csv or args.scale in ['100K', '1M', '10M']
    
    all_errors = []
    
    print(f"Generating data in {num_chunks} chunks...")
    for i in tqdm(range(num_chunks)):
        start_idx = i * chunk_size
        end_idx = min(start_idx + chunk_size, target_masters)
        chunk_lengths = lengths[start_idx:end_idx]
        
        master_df, detail_df, error_df, ml_labels, ml_anomaly = generate_chunk(
            start_master_idx=start_idx, 
            lengths=chunk_lengths, 
            seed_offset=i, 
            global_seed=global_seed,
            df_units=df_units
        )
        
        all_errors.append(error_df)
        
        # Save Parquet
        save_chunk(master_df, "MASTER", scale_dir, i, "parquet")
        save_chunk(detail_df, "DETAIL", scale_dir, i, "parquet")
        save_chunk(ml_labels, "ML_LABELS", scale_dir, i, "parquet")
        save_chunk(ml_anomaly, "ML_ANOMALY", scale_dir, i, "parquet")
        
        if gen_csv:
            save_chunk(master_df, "MASTER_CSV", scale_dir, i, "csv")
            save_chunk(detail_df, "DETAIL_CSV", scale_dir, i, "csv")

    # Save answer key
    final_errors = pd.concat(all_errors, ignore_index=True)
    ans_key_path = ans_key_dir / f"error_keys_{args.scale}.csv"
    final_errors.to_csv(ans_key_path, index=False)
    
    # Save drift point info to answer key dir as well
    with open(ans_key_dir / f"drift_info.txt", "w") as f:
        f.write(f"Drift start year: {config['dates']['drift_start_year']}\n")

    end_time = time.time()
    
    # Report sizes
    print("\n--- REPORT ---")
    print(f"Scale: {args.scale}")
    print(f"Execution time: {end_time - start_time:.2f} seconds")
    
    for tbl in ['MASTER', 'DETAIL', 'ML_LABELS', 'ML_ANOMALY', 'MASTER_CSV', 'DETAIL_CSV']:
        tbl_dir = Path(scale_dir) / tbl
        if tbl_dir.exists():
            size_mb = sum(f.stat().st_size for f in tbl_dir.glob('**/*') if f.is_file()) / (1024**2)
            print(f"Table {tbl}: {size_mb:.2f} MB")
            
    print(f"\nAnswer key generated at: {ans_key_path.absolute()}")

if __name__ == "__main__":
    main()

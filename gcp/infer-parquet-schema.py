import json
import sys
import pandas as pd

def infer_schema(parquet_file):
    try:
        # Read the Parquet file... should only use as last resort.
        df = pd.read_parquet(parquet_file)
        
        column_types = df.dtypes.apply(lambda x: str(x)).to_dict()
        columns = [{"Name": col, "Type": "string" if "object" in typ else "double" if "float" in typ else "bigint" if "int" in typ else "string"} 
                   for col, typ in column_types.items()]
        return columns
    except Exception as e:
        print(f"Error inferring schema: {e}")
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Usage: python script.py <parquet_file>")
        sys.exit(1)

    parquet_file = sys.argv[1]
    # TODO this is similar to inferring schema in main one... maybe check schema...
    schema_output = infer_schema(parquet_file)
    
    print(json.dumps(schema_output, indent=2))

if __name__ == "__main__":
    main()

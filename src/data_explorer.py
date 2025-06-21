"""
Data Structure Explorer

Quick tool to understand the structure of our raw JSON data.
"""

import json
from pathlib import Path
from collections import defaultdict


def explore_json_structure(file_path: Path, max_depth: int = 3) -> dict:
    """Explore the structure of a JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        def analyze_structure(obj, current_depth=0, path=""):
            if current_depth > max_depth:
                return {"type": type(obj).__name__, "truncated": True}

            if isinstance(obj, dict):
                result = {"type": "dict", "keys": {}}
                for key, value in obj.items():
                    new_path = f"{path}.{key}" if path else key
                    result["keys"][key] = analyze_structure(
                        value, current_depth + 1, new_path
                    )
                return result
            elif isinstance(obj, list):
                if obj:
                    return {
                        "type": "list",
                        "length": len(obj),
                        "item_structure": analyze_structure(
                            obj[0], current_depth + 1, f"{path}[0]"
                        ),
                    }
                else:
                    return {"type": "list", "length": 0}
            else:
                return {"type": type(obj).__name__, "value": str(obj)[:100]}

        return analyze_structure(data)

    except Exception as e:
        return {"error": str(e)}


def explore_entity_samples(entity: str, sample_count: int = 3):
    """Explore sample files for an entity."""
    print(f"Exploring {entity} structure")

    objects_dir = Path(f"raw/objects/{entity}")
    if not objects_dir.exists():
        print(f"No objects directory found for {entity}")
        return

    json_files = list(objects_dir.glob("*.json"))[:sample_count]

    if not json_files:
        print(f"No JSON files found for {entity}")
        return

    print(f"Found {len(list(objects_dir.glob('*.json')))} total files for {entity}")

    # Analyze structure of sample files
    for i, file_path in enumerate(json_files):
        print(f"Analyzing {entity} sample {i+1}: {file_path.name}")
        structure = explore_json_structure(file_path)

        # Print key fields we're interested in
        if "keys" in structure:
            key_fields = [
                "id",
                "externalId",
                "amount",
                "sum",
                "titleRu",
                "titleKz",
                "customerBin",
                "providerBin",
                "methodTrade",
                "startDate",
                "acceptDate",
                "paidSum",
                "externalPlanId",
                "externalTenderId",
            ]

            found_fields = {}
            for field in key_fields:
                if field in structure["keys"]:
                    found_fields[field] = structure["keys"][field]

            if found_fields:
                print(
                    f"Key fields found in {file_path.name}: {list(found_fields.keys())}"
                )

            # Show sample data
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    sample_data = json.load(f)

                sample_values = {}
                for field in key_fields:
                    if field in sample_data:
                        value = sample_data[field]
                        if isinstance(value, dict):
                            # For nested objects, show the structure
                            sample_values[field] = (
                                f"<dict with keys: {list(value.keys())[:3]}>"
                            )
                        else:
                            sample_values[field] = value

                if sample_values:
                    print(f"Sample values from {file_path.name}: {sample_values}")

            except Exception as e:
                print(f"Failed to read sample data from {file_path}: {e}")


def main():
    """Main exploration function."""
    print("=== Data Structure Exploration ===")

    entities = ["Plan", "_Lot", "OrderDetail"]

    for entity in entities:
        explore_entity_samples(entity)
        print("=" * 50)


if __name__ == "__main__":
    main()

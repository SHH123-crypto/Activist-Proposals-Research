# utils/data_utils.py
import csv
import json

def save_proposals_to_csv(proposals, filename="tally_proposals.csv"):
    keys = proposals[0].keys() if proposals else []
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(proposals)

def save_proposals_to_json(proposals, filename="tally_proposals.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(proposals, f, indent=2)

from typing import List, Dict, Any
import pandas as pd

class ReportExporter:
    def __init__(self, results: List[Dict[str, Any]]):
        self.results = results

    def to_excel(self, path: str):
        with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
            
            # Summary sheet
            summary = []
            for r in self.results:
                res = r["result"]
                summary.append({
                    "Test": r["test"],
                    "Source Count": res["source_count"],
                    "Target Count": res["target_count"],
                    "Diff Count": res["diff_count"],
                    "Missing in Target": len(res["missing_in_target"]),
                    "Missing in Source": len(res["missing_in_source"]),
                    "Dupes in Source": len(res["duplicates_in_source"]),
                    "Dupes in Target": len(res["duplicates_in_target"]),
                })
            pd.DataFrame(summary).to_excel(writer, sheet_name="Summary", index=False)

            # Per-test diffs
            for r in self.results:
                res = r["result"]
                if not res["diffs"].empty:
                    res["diffs"].to_excel(writer, sheet_name=f"{r['test'][:25]}_diffs")

            # Collated Missing & Dups
            missing_dups = []
            for r in self.results:
                test = r["test"]
                res = r["result"]

                if not res["missing_in_target"].empty:
                    tmp = res["missing_in_target"].copy()
                    tmp["Issue"] = "Missing in Target"
                    tmp["Test"] = test
                    missing_dups.append(tmp)

                if not res["missing_in_source"].empty:
                    tmp = res["missing_in_source"].copy()
                    tmp["Issue"] = "Missing in Source"
                    tmp["Test"] = test
                    missing_dups.append(tmp)

                if not res["duplicates_in_source"].empty:
                    tmp = res["duplicates_in_source"].copy()
                    tmp["Issue"] = "Dupes in Source"
                    tmp["Test"] = test
                    missing_dups.append(tmp)

                if not res["duplicates_in_target"].empty:
                    tmp = res["duplicates_in_target"].copy()
                    tmp["Issue"] = "Dupes in Target"
                    tmp["Test"] = test
                    missing_dups.append(tmp)

            if missing_dups:
                collated = pd.concat(missing_dups, ignore_index=True)
                collated.to_excel(writer, sheet_name="Missing_and_Duplicates", index=False)
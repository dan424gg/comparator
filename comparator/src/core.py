from typing import Any, Dict, List, Optional

import pandas as pd

from .data import DataSource, Normalizer


class Comparator:
    def __init__(self, pk: List[str], ignore_cols: Optional[List[str]] = None):
        self.pk = pk
        self.ignore_cols = ignore_cols or []

    def compare(self, source: pd.DataFrame, target: pd.DataFrame) -> Dict[str, Any]:
        source = source.set_index(self.pk)
        target = target.set_index(self.pk)

        # Align columns
        common_cols = [c for c in source.columns if c in target.columns and c not in self.ignore_cols]
        src = source[common_cols].sort_index().sort_index(axis=1)
        tgt = target[common_cols].sort_index().sort_index(axis=1)

        # Duplicates
        duplicates_in_source = src.index[src.index.duplicated(keep=False)]
        duplicates_in_target = tgt.index[tgt.index.duplicated(keep=False)]
        
        # Handle duplicates by converting to DataFrame first, then reset_index to avoid multiplication effect
        dupes_src = source[source.index.isin(duplicates_in_source)].reset_index()
        dupes_tgt = target[target.index.isin(duplicates_in_target)].reset_index()

        # Remove duplicates from main comparison
        src = src[~src.index.duplicated(keep='first')]
        tgt = tgt[~tgt.index.duplicated(keep='first')]

        # Missing
        missing_in_target = src.index.difference(tgt.index)
        missing_in_source = tgt.index.difference(src.index)

        missing_src = source.loc[missing_in_target].reset_index()
        missing_tgt = target.loc[missing_in_source].reset_index()

        # Clean up for diffs
        src_clean = src.drop(missing_in_target, errors='ignore')
        tgt_clean = tgt.drop(missing_in_source, errors='ignore')

        diffs = src_clean.compare(tgt_clean)

        return {
            "source_count": len(source),
            "target_count": len(target),
            "diff_count": len(diffs),
            "diffs": diffs,
            "duplicates_in_source": dupes_src,
            "duplicates_in_target": dupes_tgt,
            "missing_in_target": missing_tgt,
            "missing_in_source": missing_src,
        }

class TestCase:
    def __init__(self, name: str, source: DataSource, target: DataSource, pk: List[str],
                 normalizer: Optional[Normalizer] = None, comparator_opts=None):
        self.name = name
        self.source = source
        self.target = target
        self.normalizer = normalizer or Normalizer()
        self.comparator = Comparator(pk=pk, **(comparator_opts or {}))

    def run(self) -> Dict[str, Any]:
        src_df = self.normalizer.normalize(self.source.load())
        tgt_df = self.normalizer.normalize(self.target.load())
        result = self.comparator.compare(src_df, tgt_df)
        return {"test": self.name, "result": result}

class Session:
    def __init__(self, options: Optional[Dict[str, Any]] = None):
        self.options = options or {}
        self.tests: List[TestCase] = []

    def add_test(self, test: TestCase):
        self.tests.append(test)

    def run_all(self):
        results = []
        for test in self.tests:
            results.append(test.run())
        return results

from __future__ import annotations

import pprint
import re
from typing import Any

from kedro.io import AbstractDataCatalog, AbstractDataset
from kedro.io.core_redesign import Patterns  # noqa: TCH001

WORDS_REGEX_PATTERN = re.compile(r"\W+")


def _sub_nonword_chars(dataset_name: str) -> str:
    """Replace non-word characters in data set names since Kedro 0.16.2.

    Args:
        dataset_name: The data set name registered in the data catalog.

    Returns:
        The name used in `DataCatalog.datasets`.
    """
    return re.sub(WORDS_REGEX_PATTERN, "__", dataset_name)


class _FrozenDatasets:
    """Helper class to access underlying loaded datasets."""

    def __init__(
        self,
        *datasets_collections: _FrozenDatasets | dict[str, AbstractDataset],
    ):
        """Return a _FrozenDatasets instance from some datasets collections.
        Each collection could either be another _FrozenDatasets or a dictionary.
        """
        self._original_names: dict[str, str] = {}
        for collection in datasets_collections:
            if isinstance(collection, _FrozenDatasets):
                self.__dict__.update(collection.__dict__)
                self._original_names.update(collection._original_names)
            else:
                # Non-word characters in dataset names are replaced with `__`
                # for easy access to transcoded/prefixed datasets.
                for dataset_name, dataset in collection.items():
                    self.__dict__[_sub_nonword_chars(dataset_name)] = dataset
                    self._original_names[dataset_name] = ""

    # Don't allow users to add/change attributes on the fly
    def __setattr__(self, key: str, value: Any) -> None:
        if key == "_original_names":
            super().__setattr__(key, value)
            return
        msg = "Operation not allowed! "
        if key in self.__dict__:
            msg += "Please change datasets through configuration."
        else:
            msg += "Please use DataCatalog.add() instead."
        raise AttributeError(msg)

    def _ipython_key_completions_(self) -> list[str]:
        return list(self._original_names.keys())

    def __getitem__(self, key: str) -> Any:
        return self.__dict__[_sub_nonword_chars(key)]

    def __repr__(self) -> str:
        datasets_repr = {}
        for ds_name in self._original_names.keys():
            datasets_repr[ds_name] = self.__dict__[
                _sub_nonword_chars(ds_name)
            ].__repr__()

        return pprint.pformat(datasets_repr, sort_dicts=False)


class DataCatalogOld(AbstractDataCatalog):
    def __init__(  # noqa: PLR0913
        self,
        datasets: dict[str, AbstractDataset] | None = None,
        feed_dict: dict[str, Any] | None = None,
        dataset_patterns: Patterns | None = None,
        load_versions: dict[str, str] | None = None,
        save_version: str | None = None,
        default_pattern: Patterns | None = None,
    ) -> None:
        self._datasets = dict(datasets or {})
        self.datasets = _FrozenDatasets(self._datasets)
        # Keep a record of all patterns in the catalog.
        # {dataset pattern name : dataset pattern body}
        self._dataset_patterns = dataset_patterns or {}
        self._load_versions = load_versions or {}
        self._save_version = save_version
        self._default_pattern = default_pattern or {}

        if feed_dict:
            self.add_feed_dict(feed_dict)

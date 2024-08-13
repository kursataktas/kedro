from typing import Any

from kedro.io import AbstractDataCatalog
from kedro.io.core import (
    AbstractDataset,
    AbstractVersionedDataset,
    DatasetAlreadyExistsError,
    DatasetError,
    DatasetNotFoundError,
    Version,
)
from kedro.io.memory_dataset import MemoryDataset
from kedro.logging import _format_rich, _has_rich_handler


class KedroDataCatalog(AbstractDataCatalog):
    def __init__(  # noqa: PLR0913
        self,
        datasets: dict[str, AbstractDataset] | None = None,
        config: dict[str, dict[str, Any]] | None = None,
        credentials: dict[str, dict[str, Any]] | None = None,
        load_versions: dict[str, str] | None = None,
        save_version: str | None = None,
    ) -> None:
        self._load_versions = load_versions or {}
        self._save_version = save_version

        super().__init__(datasets, config, credentials)

        self._validate_missing_keys()

    def _validate_missing_keys(self) -> None:
        missing_keys = [
            key
            for key in self._load_versions.keys()
            if not (key in self._config or self.match_pattern(key))
        ]
        if missing_keys:
            raise DatasetNotFoundError(
                f"'load_versions' keys [{', '.join(sorted(missing_keys))}] "
                f"are not found in the catalog."
            )

    def _init_dataset(self, ds_name: str, config: dict[str, Any]):
        # Add LazyAbstractDataset to store the configuration but not to init actual dataset
        # Initialise actual dataset when load or save
        # Add is_ds_init property
        self._datasets[ds_name] = AbstractDataset.from_config(
            ds_name,
            config,
            self._load_versions.get(ds_name),
            self._save_version,
        )

    def get_dataset(
        self, ds_name: str, suggest: bool = True, version: Version | None = None
    ) -> AbstractDataset:
        dataset = super().get_dataset(ds_name, suggest)

        if version and isinstance(dataset, AbstractVersionedDataset):
            # we only want to return a similar-looking dataset,
            # not modify the one stored in the current catalog
            dataset = dataset._copy(_version=version)

        return dataset

    def add(
        self, ds_name: str, dataset: AbstractDataset, replace: bool = False
    ) -> None:
        """Adds a new ``AbstractDataset`` object to the ``KedroDataCatalog``."""
        if ds_name in self._datasets:
            if replace:
                self._logger.warning("Replacing dataset '%s'", ds_name)
            else:
                raise DatasetAlreadyExistsError(
                    f"Dataset '{ds_name}' has already been registered"
                )
        self._datasets[ds_name] = dataset
        self._resolved_ds_configs[ds_name] = {}

    def add_from_dict(self, datasets: dict[str, Any], replace: bool = False) -> None:
        for ds_name in datasets:
            if isinstance(datasets[ds_name], AbstractDataset):
                dataset = datasets[ds_name]
            else:
                dataset = MemoryDataset(data=datasets[ds_name])  # type: ignore[abstract]

            self.add(ds_name, dataset, replace)

    def load(self, name: str, version: str | None = None) -> Any:
        """Loads a registered data set.

        Args:
            name: A data set to be loaded.
            version: Optional argument for concrete data version to be loaded.
                Works only with versioned datasets.

        Returns:
            The loaded data as configured.

        Raises:
            DatasetNotFoundError: When a data set with the given name
                has not yet been registered.

        Example:
        ::

            >>> from kedro.io import DataCatalog
            >>> from kedro_datasets.pandas import CSVDataset
            >>>
            >>> cars = CSVDataset(filepath="cars.csv",
            >>>                   load_args=None,
            >>>                   save_args={"index": False})
            >>> catalog = DataCatalog(datasets={'cars': cars})
            >>>
            >>> df = catalog.load("cars")
        """
        load_version = Version(version, None) if version else None
        dataset = self.get_dataset(name, version=load_version)

        self._logger.info(
            "Loading data from %s (%s)...",
            _format_rich(name, "dark_orange")
            if _has_rich_handler(self._logger)
            else name,
            type(dataset).__name__,
            extra={"markup": True},
        )

        result = dataset.load()

        return result

    def release(self, name: str) -> None:
        """Release any cached data associated with a data set

        Args:
            name: A data set to be checked.

        Raises:
            DatasetNotFoundError: When a data set with the given name
                has not yet been registered.
        """
        dataset = self.get_dataset(name)
        dataset.release()

    def confirm(self, name: str) -> None:
        """Confirm a dataset by its name.

        Args:
            name: Name of the dataset.
        Raises:
            DatasetError: When the dataset does not have `confirm` method.

        """
        self._logger.info("Confirming dataset '%s'", name)
        dataset = self.get_dataset(name)

        if hasattr(dataset, "confirm"):
            dataset.confirm()
        else:
            raise DatasetError(f"Dataset '{name}' does not have 'confirm' method")

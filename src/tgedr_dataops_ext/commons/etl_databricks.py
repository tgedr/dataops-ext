"""Databricks-aware ETL base class.

Provides `EtlDatabricks`, an abstract intermediate class that extends the
`Etl` contract with Databricks job integration:

- Captures the Databricks `run_id` from configuration to identify the running
  job and publish outputs via ``dbutils.jobs.taskValues``.
- Exposes `_read_task_value` for consuming values published by upstream tasks,
  using the ``taskKey__key`` naming convention.
- Exposes the `inject_configuration` decorator for auto-wiring method
  parameters from the instance configuration dict or from Databricks task
  values at call time.

Concrete subclasses must implement the abstract ETL steps defined in `Etl`.
"""
import inspect
import logging
import re
from typing import Any
from collections.abc import Callable
from tgedr_dataops_abs.etl import Etl, EtlException

from tgedr_dataops_ext.commons.utils_databricks import UtilsDatabricks


logger = logging.getLogger(__name__)


class EtlDatabricks(Etl):
    """Abstract base class for ETL pipelines running on Databricks.

    Extends `Etl` with Databricks-specific behaviour:
    - Reads the Databricks job `run_id` from configuration so that task values
      can be published to downstream tasks via `dbutils.jobs.taskValues`.
    - Provides `_read_task_value` to consume values published by upstream tasks,
      following the `taskKey__key` naming convention.
    - Provides the `inject_configuration` decorator to auto-populate method
      parameters from the instance configuration dict or from Databricks task
      values (parameters named `taskKey__key` are resolved automatically when
      a valid `run_id` is present).

    Subclasses must implement the abstract methods defined in `Etl`
    (`extract`, `validate_extract`, `transform`, `validate_transform`, `load`).
    """

    _CONFIG_KEY_RUN_ID = "run_id"
    _NO_RUN_ID = "0"

    def __init__(self, configuration: dict[str, Any] | None = None) -> None:
        """Initialize the EtlDatabricks instance.

        Args:
            configuration (dict[str, Any] | None): Optional configuration dictionary. Should contain 'run_id' when running in Databricks.

        """
        super().__init__(configuration=configuration)
        # when running in databricks we should always pass `run_id` to the etl: `run_id: {{job.run_id}}`
        self._run_id: str = (
            configuration.get(self._CONFIG_KEY_RUN_ID, self._NO_RUN_ID) if configuration is not None else self._NO_RUN_ID
        )

    def run(self) -> Any:
        """Executes the ETL process: extract, validate extract, transform, validate transform, and load.

        If running in Databricks with a valid run_id, sets task values using dbutils.

        Returns:
            Any: The result of the load step, typically a dictionary of key-value pairs.
        """
        logger.info("[run|in]")

        self.extract()
        self.validate_extract()

        self.transform()
        self.validate_transform()

        result: dict[str, str] = self.load()

        if (result is not None) and (self._NO_RUN_ID != self._run_id):
            dbutils = UtilsDatabricks.get_dbutils()
            # Use .items() to iterate over key-value pairs
            for key, value in result.items():
                # Ensure dbutils is accessed as an object with attribute 'jobs'
                if hasattr(dbutils, "jobs") and hasattr(dbutils.jobs, "taskValues"): # pyright: ignore[reportAttributeAccessIssue]
                    dbutils.jobs.taskValues.set(key=key, value=value) # pyright: ignore[reportAttributeAccessIssue]
                else:
                    logger.warning("dbutils.jobs.taskValues is not accessible.")

        logger.info(f"[run|out] => {result}")
        return result

    @staticmethod
    def _read_task_value(key: str) -> str | None:
        """Reads a task value from Databricks jobs taskValues using the provided key.

        Args:
            key (str): The key in the format 'taskKey__key' to retrieve the value from Databricks taskValues.

        Returns:
            str | None: The value associated with the key, or None if not accessible.
        """
        logger.info(f"[_read_task_value|in] ({key})")
        dbutils = UtilsDatabricks.get_dbutils()
        keys: list[str] = key.split("__")
        if hasattr(dbutils, "jobs") and hasattr(dbutils.jobs, "taskValues"): # pyright: ignore[reportAttributeAccessIssue]
            result = dbutils.jobs.taskValues.get(taskKey=keys[0], key=keys[1]) # pyright: ignore[reportAttributeAccessIssue]
        else:
            result = None
            logger.warning("dbutils.jobs.taskValues is not accessible.")
        logger.info(f"[_read_task_value|out] => {result}")
        return result

    @staticmethod
    def inject_configuration(f: Callable) -> Callable:
        """Decorator to inject configuration parameters into the decorated method.

        This decorator inspects the signature of the decorated function and attempts to populate its parameters
        from the instance's configuration dictionary. If a parameter is not found in the configuration,
        its default value is used if available. For parameters matching the task value convention
        (e.g., 'namespace__param'), and if running in Databricks, the value is read from Databricks jobs taskValues.
        Raises EtlException if required parameters are missing.

        Args:
            f (Callable): The function to decorate.

        Returns:
            Callable: The decorated function with injected configuration parameters.
        """
        def decorator(self) -> Callable:  # noqa: ANN001
            signature = inspect.signature(f)

            # related to task values convention (see below)
            task_value_param_pattern = re.compile(r"\w+__\w+")

            missing_params = []
            params = {}
            # check every argument in the function signature
            for param in [parameter for parameter in signature.parameters if parameter != "self"]:

                if self._configuration is not None and param in self._configuration:
                    # if there is a configuration for the argument, we will use it
                    params[param] = self._configuration[param]
                else:
                    # if it has a default value, we will use it
                    if signature.parameters[param].default != inspect._empty:  # noqa: SLF001
                        params[param] = signature.parameters[param].default

                    # if no configuration then check if this is a task value:
                    #
                    # CONVENTION: read values from previous task values when running in databricks,
                    # we are assuming params with a namespace prefixed with two underscores (e.g.: `namespace__param`) to be task values
                    # depicting `task_name__key`
                    if (self._NO_RUN_ID != self._run_id) and (task_value_param_pattern.match(param) is not None):
                        params[param] = EtlDatabricks._read_task_value(param)

                # if no value found at this stage, we will mark it as `missing`
                if param not in params:
                    missing_params.append(param)

            if 0 < len(missing_params):
                raise EtlException(
                    f"{type(self).__name__}.{f.__name__}: missing required configuration parameters: {missing_params}"
                )

            return f(
                self,
                *[params[argument] for argument in params],
            )

        return decorator

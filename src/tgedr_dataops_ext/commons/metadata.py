"""Module for dataset metadata classes."""

from dataclasses import dataclass
import json


@dataclass(frozen=True)
class FieldFrame:
    """Class depicting a field values range, to be used in metadata.

    Parameters
    ----------
    field : str
        The name of the field.
    lower : Union[int, str, float]
        Field lower bound.
    upper : Union[int, str, float]
        Field upper bound.

    """

    __slots__ = ["field", "lower", "upper"]
    field: str
    lower: int | str | float
    upper: int | str | float

    def as_dict(self) -> dict[str, int | str | float]:
        """Convert the FieldFrame to a dictionary representation.

        Returns
        -------
        dict[str, int | str | float]
            A dictionary containing the field name, lower bound, and upper bound.
        """
        return {"field": self.field, "lower": self.lower, "upper": self.upper}

    @staticmethod
    def from_str(src: str) -> "FieldFrame":
        """Create a FieldFrame instance from a JSON string.

        Parameters
        ----------
        src : str
            A JSON string representation of a FieldFrame.

        Returns
        -------
        FieldFrame
            A new FieldFrame instance created from the JSON string.
        """
        r = json.loads(src)
        field = r["field"]
        lower = r["lower"]
        upper = r["upper"]
        return FieldFrame(field=field, lower=lower, upper=upper)

    def __str__(self) -> str:
        """Return a JSON string representation of the FieldFrame."""
        return json.dumps(self.as_dict())

    def __eq__(self, other: "FieldFrame") -> bool:
        """Check equality between two FieldFrame instances."""
        return self.field == other.field and self.lower == other.lower and self.upper == other.upper

    def __gt__(self, other: "FieldFrame") -> bool:
        """Check if this FieldFrame is greater than another FieldFrame."""
        return self.field > other.field or (
            self.field == other.field
            and (self.lower > other.lower or (self.lower == other.lower and self.upper > other.upper))
        )

    def __ne__(self, other: "FieldFrame") -> bool:
        """Check inequality between two FieldFrame instances."""
        return not other == self

    def __ge__(self, other: "FieldFrame") -> bool:
        """Check if this FieldFrame is greater than or equal to another FieldFrame."""
        return other == self or self > other

    def __le__(self, other: "FieldFrame") -> bool:
        """Check if this FieldFrame is less than or equal to another FieldFrame."""
        return other == self or self < other

    def __lt__(self, other: "FieldFrame") -> bool:
        """Check if this FieldFrame is less than another FieldFrame."""
        return other > self


@dataclass(frozen=True)
class Metadata:
    """Immutable class depicting dataset metadata.

    Parameters
    ----------
    name : str
        The name of the dataset.
    version : Optional[str]
        Version of this dataset, if available.
    framing : Optional[List[FieldFrame]]
        Multiple field frames.
    sources : Optional[List["Metadata"]]
        Metadatas related to the datasets sourcing this one.

    """

    __slots__ = ["framing", "name", "sources", "version"]
    name: str
    version: str | None
    framing: list[FieldFrame] | None
    sources: list["Metadata"] | None

    def as_dict(self) -> dict:
        """Convert the Metadata to a dictionary representation.

        Returns
        -------
        dict
            A dictionary containing the metadata fields including name, version,
            framing, and sources if they are not None.
        """
        result = {"name": self.name}
        if self.version is not None:
            result["version"] = self.version
        if self.framing is not None:
            result["framing"] = []
            for f in self.framing:
                (result["framing"]).append(f.as_dict())
        if self.sources is not None:
            result["sources"] = []
            for source in self.sources:
                (result["sources"]).append(source.as_dict())

        return result

    def __str__(self) -> str:
        """Return a JSON string representation of the Metadata."""
        return json.dumps(self.as_dict())

    def __eq__(self, other: object) -> bool:
        """Check equality between two Metadata instances."""
        return (
            self.name == other.name
            and (
                (self.version is None and other.version is None)
                or ((self.version is not None and other.version is not None) and self.version == other.version)
            )
            and (
                (self.framing is None and other.framing is None)
                or (
                    (self.framing is not None and other.framing is not None)
                    and sorted(self.framing) == sorted(other.framing)
                )
            )
            and (
                (self.sources is None and other.sources is None)
                or (
                    (self.sources is not None and other.sources is not None)
                    and sorted(self.sources) == sorted(other.sources)
                )
            )
        )

    def __gt__(self, other: "Metadata") -> bool:
        """Check if this Metadata is greater than another Metadata."""
        return self.name > other.name or (
            self.name == other.name
            and (
                ((self.version is not None and other.version is not None) and (self.version > other.version))
                or (self.version is not None and other.version is None)
                or (
                    (
                        (self.framing is not None and other.framing is not None)
                        and (sorted(self.framing) > sorted(other.framing))
                    )
                    or (self.framing is not None and other.framing is None)
                    or (
                        (
                            (self.sources is not None and other.sources is not None)
                            and (sorted(self.sources) > sorted(other.sources))
                        )
                        or (self.sources is not None and other.sources is None)
                    )
                )
            )
        )

    def __ne__(self, other: "Metadata") -> bool:
        """Check inequality between two Metadata instances."""
        return not other == self

    def __ge__(self, other: "Metadata") -> bool:
        """Check if this Metadata is greater than or equal to another Metadata."""
        return other == self or self > other

    def __le__(self, other: "Metadata") -> bool:
        """Check if this Metadata is less than or equal to another Metadata."""
        return other == self or self < other

    def __lt__(self, other: "Metadata") -> bool:
        """Check if this Metadata is less than another Metadata."""
        return other > self

    @staticmethod
    def from_str(src: str) -> "Metadata":
        """Create a Metadata instance from a JSON string.

        Parameters
        ----------
        src : str
            A JSON string representation of a Metadata object.

        Returns
        -------
        Metadata
            A new Metadata instance created from the JSON string.
        """
        r = json.loads(src)
        name = r["name"]
        version = r.get("version", None)

        framing = None
        framing_entries = r.get("framing", None)
        if framing_entries is not None:
            framing = []
            for framing_entry in framing_entries:
                framing.append(FieldFrame.from_str(json.dumps(framing_entry)))

        sources = None
        sources_entries = r.get("sources", None)
        if sources_entries is not None:
            sources = []
            for source_entry in sources_entries:
                source_entry_as_str = json.dumps(source_entry)
                sources.append(Metadata.from_str(source_entry_as_str))

        return Metadata(name=name, version=version, framing=framing, sources=sources)

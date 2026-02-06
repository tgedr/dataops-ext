from tgedr_dataops_ext.commons.metadata import FieldFrame, Metadata


def test_fieldframe():
    o = FieldFrame(field="field", lower=1.0, upper=3.23)
    assert o == FieldFrame.from_str(str(o))

    o2 = FieldFrame(field="field", lower=1.0, upper=3.2)
    assert o != o2

    o2 = FieldFrame(field="field", lower=1.0, upper=3.23)
    assert o == o2

    o2 = FieldFrame(field="fiela", lower=1.0, upper=3.23)
    assert o >= o2

    o2 = FieldFrame(field="fiela", lower=1.0, upper=3.23)
    assert o > o2

    o2 = FieldFrame(field="fielx", lower=1.0, upper=3.23)
    assert o <= o2

    o2 = FieldFrame(field="fielx", lower=1.0, upper=3.23)
    assert o < o2

    o2 = FieldFrame(field="field", lower=0.05, upper=3.23)
    assert o >= o2

    o2 = FieldFrame(field="field", lower=0.05, upper=3.23)
    assert o > o2

    o2 = FieldFrame(field="field", lower=1.05, upper=3.23)
    assert o <= o2

    o2 = FieldFrame(field="field", lower=1.05, upper=3.23)
    assert o < o2

    o2 = FieldFrame(field="field", lower=1.0, upper=3.2)
    assert o >= o2

    o2 = FieldFrame(field="field", lower=1.0, upper=3.2)
    assert o > o2

    o2 = FieldFrame(field="field", lower=1.0, upper=3.24)
    assert o <= o2

    o2 = FieldFrame(field="field", lower=1.0, upper=3.24)
    assert o < o2


def test_metadata():
    o = Metadata(
        name="tableX", version="version", framing=[FieldFrame(field="field", lower=1.0, upper=3.23)], sources=None
    )
    assert o == Metadata.from_str(str(o))

    o = Metadata(
        name="tableX",
        version="version",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=[
            Metadata(
                name="tableA",
                version="version",
                framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
                sources=None,
            )
        ],
    )
    assert o == Metadata.from_str(str(o))

    o = Metadata(
        name="tableX",
        version="version",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=[
            Metadata(
                name="tableA",
                version="version",
                framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
                sources=None,
            )
        ],
    )
    o2 = Metadata(
        name="tableX",
        version="version",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=[
            Metadata(
                name="tableA",
                version="version",
                framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
                sources=None,
            )
        ],
    )

    assert o == o2

    o = Metadata(
        name="table",
        version="version",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=[
            Metadata(
                name="tableA",
                version="version",
                framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
                sources=None,
            )
        ],
    )
    o2 = Metadata(
        name="tableX",
        version="version",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=[
            Metadata(
                name="tableA",
                version="version",
                framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
                sources=None,
            )
        ],
    )

    assert o <= o2

    o = Metadata(
        name="tableX",
        version="version",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=[
            Metadata(
                name="tableA",
                version="version",
                framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
                sources=None,
            )
        ],
    )
    o2 = Metadata(
        name="tableX",
        version="versiom",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=[
            Metadata(
                name="tableA",
                version="version",
                framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
                sources=None,
            )
        ],
    )

    assert o >= o2

    o = Metadata(
        name="tableX",
        version="version",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=[
            Metadata(
                name="tableA",
                version="version",
                framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
                sources=None,
            )
        ],
    )
    o2 = Metadata(
        name="tableX",
        version="version",
        framing=[FieldFrame(field="fiela", lower=1.0, upper=3.23)],
        sources=[
            Metadata(
                name="tableA",
                version="version",
                framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
                sources=None,
            )
        ],
    )

    assert o > o2


def test_metadata_ne_and_le():
    """Test Metadata __ne__ and __le__ methods"""
    o = Metadata(
        name="tableX",
        version="version",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=None,
    )
    o2 = Metadata(
        name="tableY",
        version="version",
        framing=[FieldFrame(field="field", lower=1.0, upper=3.23)],
        sources=None,
    )
    
    # Test __ne__
    assert o != o2
    
    # Test __le__
    assert o <= o2
    assert o2 >= o

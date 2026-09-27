def build_gold(valid_df):
    """
    Build the versioned Gold university chapters data product.

    Only records that passed hard DQ reach this function.
    WARNING records remain publishable as required by the contract.
    """

    return valid_df.select(
        "chapter_id",
        "chapter_name",
        "city",
        "state",
        "longitude",
        "latitude",
        "dq_status",
        "dq_warnings",
    )

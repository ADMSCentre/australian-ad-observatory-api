mapping = [
    {
        "from": "version",
        "to": "version",
    },
    # Keep all .uuid fields
    {
        "from": "*.uuid",
        "to": "*.uuid",
    },
    # Keep all fields from the 'observer' object
    {
        "from": "observer.*",
        "to": "observer.*",
        "transform": "NULL_STRING_TO_NONE"
    },
    # These fields need to be transformed to strings for the index to work properly
    {
        "from": "observer.demographic_characteristics.study_related.online_frauds_or_scams.been_victim_of_online_fraud_or_scams",
        "to": "observer.demographic_characteristics.study_related.online_frauds_or_scams.been_victim_of_online_fraud_or_scams",
        "transform": "TO_STRING"
    },
    {
        "from": "observer.demographic_characteristics.study_related.relationships_and_fertility.pregnancies_that_resulted_in_live_birth_specified",
        "to": "observer.demographic_characteristics.study_related.relationships_and_fertility.pregnancies_that_resulted_in_live_birth_specified",
        "transform": "TO_STRING"
    },
    # Other fields
    {
        "from": "observation.observed_on_device_at",
        "to": "observation.observed_on_device_at",
    },
    {
        "from": "observation.submitted_from_device_at",
        "to": "observation.submitted_from_device_at",
    },
    {
        "from": "observation.platform",
        "to": "observation.platform",
    },
    {
        "from": "observation.ad_format",
        "to": "observation.ad_format",
    },
    {
        "from": "observation.keyframes.[i].observed_at",
        "to": "observation.keyframes.[i].observed_at",
    },
    {
        "from": "observation.keyframes.[i].ocr_data.[j].text",
        "to": "observation.keyframes.[i].ocr_data.[j].text",
    },
    {
        "from": "observation.keyframes.[i].ocr_data.[j].confidence",
        "to": "observation.keyframes.[i].ocr_data.[j].confidence",
    }
]

from humps import camelize
from opensearch_dsl.response.hit import Hit


def csv_serialize(record: Hit) -> dict:
    """Serialize the DOI, state, client_id, and updated date from an OpenSearch record to a dictionary for the CSV file

    Args:
        record (dict): OpenSearch record

    Returns:
        dict: Serialized record
    """
    return {
        "doi": record.uid,
        "state": record.aasm_state,
        "client_id": record.client_id,
        "updated": record.updated
    }


def json_serialize(record: Hit) -> dict:
    """Apply normalizations to an OpenSearch record to match the REST API output

    Args:
        record (dict): OpenSearch record

    Returns:
        dict: Serialized record
    """
    # Convert record to dictionary
    record = record.to_dict()

    # Extract keys that live outside `attributes`
    client_id = record.pop("client_id")
    provider_id = record.pop("provider_id")
    media_ids = record.pop("media_ids")
    reference_ids = record.pop("reference_ids")
    citation_ids = record.pop("citation_ids")
    part_ids = record.pop("part_ids")
    part_of_ids = record.pop("part_of_ids")
    version_ids = record.pop("version_ids")
    version_of_ids = record.pop("version_of_ids")

    # Rename keys that have a different name in OpenSearch
    record["doi"] = record.pop("uid") # Use `uid` because it is lowercased
    record["publisher"] = record.pop("publisher_obj")
    record["version"] = record.pop("version_info")
    record["state"] = record.pop("aasm_state")

    # CamelCase the keys in the record
    record = camelize(record)

    # Wrap array fields
    record = wrap_array_fields(record)
    # Populate empty fields
    record = populate_empty_fields(record)
    # Populate `published` field
    record = populate_published(record)
    # Populate `identifiers` field
    record = populate_identifiers(record)
    # Populate `alternateIdentifiers` field
    record = populate_alternate_identifiers(record)
    # Convert `isActive` field
    record = convert_is_active(record)

    # Wrap the record in the same format as the REST API response
    serialized_record = {
        "id": record["doi"],
        "type": "dois",
        "attributes": record,
        "relationships": {
            "client": {
                "data": {
                    "id": client_id,
                    "type": "clients"
                }
            },
            "provider": {
                "data": {
                    "id": provider_id,
                    "type": "providers"
                }
            },
            "media": {
                "data": [
                    {"id": media_id, "type": "dois"} for media_id in media_ids
                ]
            },
            "references": {
                "data": [
                    {"id": reference_id, "type": "dois"} for reference_id in reference_ids
                ]
            },
            "citations": {
                "data": [
                    {"id": citation_id, "type": "dois"} for citation_id in citation_ids
                ]
            },
            "parts": {
                "data": [
                    {"id": part_id, "type": "dois"} for part_id in part_ids
                ]
            },
            "partOf": {
                "data": [
                    {"id": part_of_id, "type": "dois"} for part_of_id in part_of_ids
                ]
            },
            "versions": {
                "data": [
                    {"id": version_id, "type": "dois"} for version_id in version_ids
                ]
            },
            "versionOf": {
                "data": [
                    {"id": version_of_id, "type": "dois"} for version_of_id in version_of_ids
                ]
            }
        }
    }
    return serialized_record


def wrap_array_fields(record: dict) -> dict:
    """Ensure that fields which should be arrays are.

    Args:
        record (dict): Record to be wrapped

    Returns:
        dict: Wrapped record
    """
    array_fields = ["creators", "contributors", "rightsList", "fundingReferences", "identifiers",
                    "relatedIdentifiers", "relatedItems", "geoLocations", "dates", "subjects",
                    "sizes", "titles", "descriptions", "formats"]

    for field in array_fields:
        if field in record and not isinstance(record[field], list):
            if record[field]:
                record[field] = [record[field]]
            else:
                record[field] = []
    return record


def populate_empty_fields(record: dict) -> dict:
    """
    Populate empty fields in the record.

    Args:
        record (dict): Record to be populated

    Returns:
        dict: Populated record
    """
    dictionary_fields = ["container", "types"]

    for field in dictionary_fields:
        if field not in record or not record[field]:
            record[field] = {}

    return record


def populate_published(record: dict) -> dict:
    """
    Populate the "published" field in a record according to the following rules:
    1. Check for the first date with type "Issued" and use that date.
    2. If no such date is found, use the publication year as a string.

    Ruby code:
        get_date(dates, "issued") | | publication_year.to_s

    Args:
        record (dict): Record to be populated.

    Returns:
        dict: Populated record.
    """
    p_date = None

    for date in record["dates"]:
        if "dateType" in date and date["dateType"] in ["Issued", "issued"]:
            p_date = date.get("date", None)
            break

    if not p_date and "publicationYear" in record and record["publicationYear"]:
        p_date = str(record["publicationYear"])

    record["published"] = p_date
    return record


def populate_identifiers(record: dict) -> dict:
    """
    Populate the "identifiers" field in a record according to the following rules:
    1. Remove "identifier" values that are equal to the "doi" or "url" of the record.

    Ruby code:
        Array.wrap(object.identifiers).select do |r|
          [object.doi, object.url].exclude?(r["identifier"])
        end

    Args:
        record (dict): Record to be populated.

    Returns:
        dict: Populated record.
    """

    record["identifiers"] = [r for r in record["identifiers"] if r.get("identifier", None) not in [record["doi"], record["url"]]]
    return record

def populate_alternate_identifiers(record: dict) -> dict:
    """
    Populate the "alternateIdentifiers" field in a record according to the following rules:
    1. Remove "identifier" values that are equal to the "doi" or "url" of the record.
    2. Map the resulting list of identifiers to a list of dictionaries with keys "alternateIdentifierType" and "alternateIdentifier".

    Ruby code:
        Array.wrap(object.identifiers).select do | r |
          [object.doi, object.url].exclude?(r["identifier"])
        end.map do | a |
          {
            "alternateIdentifierType" => a["identifierType"],
            "alternateIdentifier" => a["identifier"],
          }
        end.compact

    Args:
        record (dict): Record to be populated.

    Returns:
        dict: Populated record.
    """
    record["alternateIdentifiers"] = [
        {
            "alternateIdentifierType": r.get("identifierType", None),
            "alternateIdentifier": r.get("identifier", None),
        }
        for r in record["identifiers"]
        if r["identifier"] not in [record["doi"], record["url"]]
    ]
    return record


def convert_is_active(record: dict) -> dict:
    """
    Convert the "isActive" field to a boolean value by checking whether the first byte of the UTF-8 encoding of the string is equal to 1.

    Ruby code:
        object.is_active.to_s.getbyte(0) == 1

    Args:
        record (dict): Record to be converted.

    Returns:
        dict: Converted record.
    """
    record["isActive"] = bytes(record["isActive"], "utf-8")[0] == 1
    return record

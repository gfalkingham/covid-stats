from typing import Iterable, Dict, Union, List
from json import dumps
from requests import get
from http import HTTPStatus


StructureType = Dict[str, Union[dict, str]]
FiltersType = Iterable[str]
APIResponseType = Union[List[StructureType], str]


def get_paginated_dataset(filters: FiltersType, structure: StructureType) -> APIResponseType:
    """
    Extracts paginated data by requesting all of the pages
    and combining the results.

    Parameters
    ----------
    filters: Iterable[str]
        API filters. See the API documentations for additional
        information.

    structure: Dict[str, Union[dict, str]]
        Structure parameter. See the API documentations for
        additional information.

    Returns
    -------
    Union[List[StructureType], str]
        Comprehensive list of dictionaries containing all the data for
        the given ``filters`` and ``structure``.
    """
    endpoint = "https://api.coronavirus.data.gov.uk/v1/data"

    api_params = {
        "filters": str.join(";", filters),
        "structure": dumps(structure, separators=(",", ":")),
        "format": "csv"
    }

    data = list()

    page_number = 1

    while True:
        # Adding page number to query params
        api_params["page"] = page_number

        response = get(endpoint, params=api_params, timeout=10)

        if response.status_code >= HTTPStatus.BAD_REQUEST:
            raise RuntimeError(f'Request failed: {response.text}')
        elif response.status_code == HTTPStatus.NO_CONTENT:
            break

        csv_content = response.content.decode()
        
        # Removing CSV header (column names) where page
        # number is greater than 1.
        if page_number > 1:
            data_lines = csv_content.split("\n")[1:]
            csv_content = str.join("\n", data_lines)
        
        data.append(csv_content.strip())
        page_number += 1
        continue

        current_data = response.json()
        page_data: List[StructureType] = current_data['data']

        data.extend(page_data)

        # The "next" attribute in "pagination" will be `None`
        # when we reach the end.
        if current_data["pagination"]["next"] is None:
            break

        page_number += 1

    # Concatenating CSV pages
    return str.join("\n", data)


if __name__ == "__main__":
    query_filters = [
        f"areaType=overview"
    ]

    query_structure = {
        "date": "date",
        "daily": "newCasesByPublishDate"
    }

    csv_data = get_paginated_dataset(query_filters, query_structure)
    csv_lines = csv_data.split("\n")
#    print("CSV:")
#    print(f"Length:", len(csv_lines))
#    print("Data (first 3 lines):", csv_lines[:3])

    with open('stats.csv', 'w', newline='') as file:
        file.write(csv_data)

from typing import Union, Dict, List


def get_columns_from_schema(
    column_name: str,
    columns: List[Dict],
    return_type: str,
    dict_first_column: str = "name",
    dict_second_column: str = None,
    ignore_column: str = "",
    list_column: str = "name",
) -> Union[List[str], Dict[str, str]]:
    if return_type == "dict":
        column_list = {}
    elif return_type == "list":
        column_list = []

    for column in columns:
        column_info = column.get(column_name)
        if column.get(ignore_column) == True:
            continue
        if column_info:
            if isinstance(column_list, dict):
                if isinstance(column_info, bool):
                    column_list[column[dict_first_column]] = column[dict_second_column]
                else:
                    column_list[column[column_name]] = column[dict_second_column]
            elif isinstance(column_list, list):
                column_list.append(column[list_column])

    return column_list


def get_columns_from_schema_as_list(
    column_name: str,
    columns: List[Dict],
    list_column: str = "name",
    ignore_columns: Union[List[str], str] = None,
) -> List[str]:
    column_list = []
    if ignore_columns is None:
        ignore_columns = []

    if isinstance(ignore_columns, str):
        ignore_columns = [ignore_columns]

    for column in columns:
        column_info = column.get(column_name)
        if any(column.get(ignore_column) for ignore_column in ignore_columns):
            continue

        if column_info:
            if isinstance(column_info, bool):
                column_list.append(column[list_column])
            else:
                column_list.append(column[column_name])

    return column_list


def get_columns_from_schema_as_dict(
    column_name: str,
    columns: List[Dict],
    dict_first_column: str = "name",
    dict_second_column: str = None,
    ignore_columns: Union[List[str], str] = None,
) -> Dict[str, str]:
    column_dict = {}
    if ignore_columns is None:
        ignore_columns = []

    for column in columns:
        column_info = column.get(column_name) or column.get("custom_configs", {}).get(
            column_name
        )
        if any(column.get(ignore_column) for ignore_column in ignore_columns):
            continue
        if column_info:
            if isinstance(column_info, bool):
                column_dict[column[dict_first_column]] = column[dict_second_column]
            else:
                column_dict[column[column_name]] = column[dict_second_column]

    return column_dict

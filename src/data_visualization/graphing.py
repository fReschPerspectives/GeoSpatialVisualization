import pandas as pd
import seaborn as sns
import typing


def create_barplot(df: pd.DataFrame, cat: str, quant: str, limit: int = 10, sort_by: str = "desc" ) -> None:
    """

    :param df: dataframe to create the relevant bar plot
    :param cat: category column
    :param quant: quantity column
    :param limit: limit of the number of categories to plot
    :param sort_by: sort by "desc" or "asc"
    :return: None
    """
    if sort_by not in ["desc", "asc"]:
        raise ValueError("sort_by must be either 'desc' or 'asc'")
    elif sort_by == "desc":
        sort_flag = False
    else:
        sort_flag = True
    sns.barplot(df.sort_values(by="Change", axis=0, ascending = False).head(limit), y = cat, x = quant, hue = cat, orient = 'h')
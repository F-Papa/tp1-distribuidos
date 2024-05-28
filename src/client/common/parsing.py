def parse_book_line(line: dict) -> dict:
    return {
        "title": line["Title"],
        "year": _parse_year(line["publishedDate"]),
        "authors": _parse_authors_array(line["authors"]),
        "publisher": line["publisher"],
        "categories": _parse_category_array(line["categories"]),
    }

def parse_review_line(line: dict) -> dict:
    return {
        "title": line["Title"],
        "review/score": line["review/score"],
        "review/text": line["review/text"],
    }

# Parses the year from the csv file to an integer
def _parse_year(year_str: str) -> int:
    if year_str == "":
        return 0
    year_str = year_str.replace("*", "")
    year_str = year_str.replace("?", "0")
    if "-" in year_str:
        return int(year_str.split("-")[0])
    else:
        return int(year_str)

# Parses the authors from the csv file to a list of strings
def _parse_authors_array(authors_str: str) -> list:

    authors_str = (
        authors_str.replace('"[', "")
        .replace(']"', "")
        .replace("[", "")
        .replace("]", "")
        .replace(", ", ",")
    )

    if '"' in authors_str:
        authors_str = authors_str.replace('"', "")
    else:
        authors_str = authors_str.replace("'", "")

    return authors_str.split(",")

# Parses the categories from the csv file to a list of strings
def _parse_category_array(categories_str: str) -> list:
    categories_str = categories_str.replace("['", "").replace("']", "")
    return categories_str.split(" & ")

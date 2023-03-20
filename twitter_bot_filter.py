import csv
import json
import logging as log
import re
from argparse import ArgumentParser
from dataclasses import dataclass
from functools import cache
from typing import Any, Callable, Optional

from botometer import Botometer
from camel_converter.pydantic_base import CamelBase
from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class TwitterBotFilterArgs:
    file_in: str
    file_out: Optional[str]
    config_filename: str
    csv_url_header: str


class TwitterAuth(CamelBase):
    consumer_key: str
    consumer_secret: str
    access_token: str
    access_token_secret: str

    class Config:
        frozen = True


class FilterConfig(CamelBase):
    botometer_api_key: str
    twitter_auth: TwitterAuth

    class Config:
        frozen = True


class BotometerConnection:
    __bom: Botometer
    __weighting: float

    def __init__(self, api_key: str, twitter_auth: TwitterAuth):
        self.__bom = Botometer(wait_on_ratelimit=True, rapidapi_key=api_key, **twitter_auth.dict())  # type: ignore
        self.__weighting = 0.8

    def set_bot_cap(self, weighting: float):
        # minimum probability needed to consider an account as a bot
        if weighting >= 0 or weighting <= 1:
            self.__weighting = weighting

    @cache
    def account_is_human(self, account_name: str) -> bool:
        result = self.__bom.check_account(account_name)

        # is human if conditional probability of the account being a bot is less than the weighting
        return result.cap.english <= self.__weighting


@dataclass(frozen=True)
class CsvData:
    fieldnames: list[str]
    data: list[dict[Any, Any]]


def read_csv_data(csv_filename_in: str) -> Optional[CsvData]:
    with open(csv_filename_in, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        header = reader.fieldnames

        if header is None:
            log.error("Unable to read csv header for file %s", csv_filename_in)
            return None

        fieldnames = [field for field in header]
        rows = [row for row in reader]

    return CsvData(fieldnames, rows)


def write_csv_data(csv_data: CsvData, csv_filename_out: str):
    with open(csv_filename_out, mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_data.fieldnames)
        writer.writerows(csv_data.data)


def filter_csv_data(
    csv_data: CsvData, filter_func: Callable[[dict[Any, Any]], bool]
) -> CsvData:
    return CsvData(
        csv_data.fieldnames, [row for row in csv_data.data if filter_func(row)]
    )


def filter_csv(
    csv_filename_in: str,
    csv_filename_out: Optional[str],
    csv_url_header: str,
    botometer_api_key: str,
    twitter_auth: TwitterAuth,
) -> bool:
    bom = BotometerConnection(botometer_api_key, twitter_auth)

    URI_REGEX = re.compile("twitter.com/(\\w{1,15})/")

    def get_username(uri: str) -> Optional[str]:
        match = URI_REGEX.search(uri)

        if match is None:
            return None

        return match.group(1)

    def filter_by_bottines(data: dict[Any, Any]) -> bool:
        username = get_username(data[csv_url_header])

        if username is None:
            log.warning("no username found within row %s", data)

        if bom.account_is_human(username):
            log.info("@%s seems to be a human", username)
            return True
        else:
            log.info("@%s account seems to be a bot", username)
            return False

    csv_data = read_csv_data(csv_filename_in)

    if csv_data is None:
        return False

    filtered_csv_data = filter_csv_data(csv_data, filter_by_bottines)

    if csv_filename_out:
        write_csv_data(filtered_csv_data, csv_filename_out)

    return True


def __parse_args() -> TwitterBotFilterArgs:
    parser = ArgumentParser(
        description="Python script used to filter out twitter bots from a csv"
    )
    parser.add_argument(
        "file_in",
        help="path to the csv file",
    )
    parser.add_argument(
        "csv_url_header",
        help="csv header indicator for twitter url link",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="path to output the filtered csv file",
        required=False,
    )
    parser.add_argument(
        "-c",
        "--config",
        default="filterConfig.json",
        help="config file location",
    )

    parser_args = parser.parse_args()

    return TwitterBotFilterArgs(
        file_in=parser_args.file_in,
        file_out=parser_args.output,
        config_filename=parser_args.config,
        csv_url_header=parser_args.csv_url_header,
    )


def __read_config(config_filename: str) -> FilterConfig:
    with open(config_filename) as filter_config:
        return FilterConfig(**json.load(filter_config))


if __name__ == "__main__":
    filter_args = __parse_args()
    config = __read_config(filter_args.config_filename)
    filtering_successful = filter_csv(
        csv_filename_in=filter_args.file_in,
        csv_filename_out=filter_args.file_out,
        csv_url_header=filter_args.csv_url_header,
        botometer_api_key=config.botometer_api_key,
        twitter_auth=config.twitter_auth,
    )

    if filtering_successful:
        log.info("Filtering successful")
        exit(0)
    else:
        log.info("Filtering unsuccessful. See error logs")
        exit(1)

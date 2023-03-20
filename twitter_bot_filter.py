import csv
import json
import logging
import re
from argparse import ArgumentParser
from dataclasses import dataclass
from functools import cache
from typing import Optional

from botometer import Botometer
from camel_converter.pydantic_base import CamelBase
from pydantic.dataclasses import dataclass


@dataclass(frozen=True)
class TwitterBotFilterArgs:
    file_in: str
    file_out: Optional[str]
    config_filename: str


class TwitterAuth(CamelBase):
    consumer_key: str
    consumer_secret: str
    access_token: str
    access_token_secret: str


class FilterConfig(CamelBase):
    botometer_api_key: str
    csv_url_header: str
    twitter_auth: TwitterAuth

    class Config:
        frozen = True


class BotometerConnection:
    __bom: Botometer

    def __init__(self, api_key: str, twitter_auth: TwitterAuth):
        self.__bom = Botometer(wait_on_ratelimit=True, rapidapi_key=api_key, **twitter_auth.dict())  # type: ignore

    @cache
    def account_is_human(self, account_name: str) -> bool:
        result = self.__bom.check_account(account_name)

        print(result)

        return True


def filter_csv(
    csv_filename_in: str,
    csv_filename_out: str,
    csv_url_header: str,
    botometer_api_key: str,
    twitter_auth: TwitterAuth,
):
    bom = BotometerConnection(botometer_api_key, twitter_auth)

    URI_REGEX = re.compile("twitter.com/(\\w{1,15})/")

    def get_username(uri: str) -> Optional[str]:
        match = URI_REGEX.search(uri)

        if match is None:
            return None

        return match.group(1)

    # read & filter
    with open(csv_filename_in, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        header = reader.fieldnames

        rows = []
        for row in reader:
            username = get_username(row[csv_url_header])

            if username is None:
                logging.warn("no username found within row", row)

            if bom.account_is_human(username):
                rows.append(row)
            else:
                logging.info("account seems to be a bot", username)

    # write
    with open(csv_filename_out, mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header if header else [])
        writer.writerows(rows)


def __parse_args() -> TwitterBotFilterArgs:
    parser = ArgumentParser(
        description="Python script used to filter out twitter bots from a csv"
    )
    parser.add_argument(
        "file_in",
        help="path to the csv file",
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
    )


def __read_config(config_filename: str) -> FilterConfig:
    with open(config_filename) as filter_config:
        return FilterConfig(**json.load(filter_config))


if __name__ == "__main__":
    filter_args = __parse_args()
    config = __read_config(filter_args.config_filename)
    filter_csv(
        csv_filename_in=filter_args.file_in,
        csv_filename_out=filter_args.file_out,
        csv_url_header=config.csv_url_header,
        botometer_api_key=config.botometer_api_key,
        twitter_auth=config.twitter_auth,
    )

import csv
import re

from botometer import Botometer

URI_REGEX = re.compile("twitter.com/(\\w{1,15})/")


def get_username_from_url(uri):
    match = URI_REGEX.search(uri)

    if match is None:
        return None

    return match.group(1)


def filter_csv(
    csv_filename_in,
    csv_filename_out,
    csv_url_header,
    botometer_api_key,
    twitter_auth,
):
    bom = Botometer(
        wait_on_ratelimit=True,
        rapidapi_key=botometer_api_key,
        **twitter_auth,
    )

    # read and filter
    with open(csv_filename_in, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        header = reader.fieldnames

        rows = []
        for row in reader:
            username = get_username_from_url(row[csv_url_header])

            MAX_BOTTINESS = 0.8
            account_result = bom.check_account(username)

            # account is human if their responses aren't completely botty
            if account_result.cap.english <= MAX_BOTTINESS:
                rows.append(row)

    # check if csv file has a header
    if header is None:
        print(
            "No header found within given csv file: "
            + csv_filename_in
            + ". cancelling write"
        )
        return

    # write human rows to new csv file
    with open(csv_filename_out, mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writerows(rows)


if __name__ == "__main__":
    filter_csv(
        csv_filename_in="file_in.csv",
        csv_filename_out="file_out.csv",
        csv_url_header="csv_header_name",
        botometer_api_key="botometer_api_key",
        twitter_auth={
            "consumer_key": "consumerKey",
            "consumer_secret": "consumerSecret",
            "access_token": "accessToken",
            "access_token_secret": "accessTokenSecret",
        },
    )

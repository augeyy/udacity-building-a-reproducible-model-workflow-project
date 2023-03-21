#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import os
import tempfile

import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logger.info("Download artifact from W&B...")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Reading CSV...")
    df = pd.read_csv(artifact_local_path)

    # Drop outliers
    logger.info("Dropping outliers...")
    min_price = 10
    max_price = 350
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()

    # Convert last_review to datetime
    logger.info("Converting `last_review` to datetime...")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save the artifact
    logger.info("Uploading artifact to W&B...")
    with tempfile.TemporaryDirectory() as tmp_dir:

        temp_fpath = os.path.join(tmp_dir, args.output_artifact)
        df.to_csv(temp_fpath, index=False)

        artifact = wandb.Artifact(
            name=args.output_artifact,
            type=args.output_type,
            description=args.output_description,
        )
        artifact.add_file(temp_fpath)
        run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name for the W&B output artifact that will be created",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description for the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Keep all rows for which the `price` value is equal or superior to this value",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Keep all rows for which the `price` value is equal or inferior to this value",
        required=True
    )

    args = parser.parse_args()

    go(args)

# # from kpi import dispatch
from kpi.constants import BUNDLE, TABLES
from kpi.api.api_base import API
# from kpi import user_info
import os
import click
import shutil
import bcolz
import warnings
import tqdm

# bcolz has deprecation warning with pandas.lib
warnings.filterwarnings("ignore", category=FutureWarning)


@click.group()
def main():
    # // TODO initialize the logger here
    # print(BUNDLE)
    # print(os.path.exists(BUNDLE))
    pass


@main.command()
@click.option(
    '-o',
    '--overwrite',
    default=True,
    help="Remove the previous bundles while creating a new one"
)
def migrate(overwrite):
    """
    It removes the previous bundles before creating a
    new bundles if the overwrite option is true. Otherwise
    it keeps the tables and create only those tables that are
    affected by the command. Everytime the database is updated,
    we need to run the migrate to affect newly made changes.
    """
    if overwrite:
        # First remove the old bundles
        shutil.rmtree(BUNDLE, ignore_errors=True)
        # Create new bundle
        os.makedirs(BUNDLE)
        for each in tqdm.tqdm(TABLES):
            data = API(base='django')[each]
            bcolz.ctable.fromdataframe(
                data, rootdir=os.path.join(BUNDLE, each))


if __name__ == '__main__':
    main()

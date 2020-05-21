import click
from tabulate import tabulate


from databricks_cli.configure.provider import get_all_profiles
from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.version import print_version_callback, version


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Lists all profiles, hosts, and usernames present in the config file.')
@click.option('--quiet', '-q', show_default=True, is_flag=True, default=False,
              help='Only show profile names.')
def list_cli(quiet):
    """
    Lists all profiles, hosts, and usernames present in the config file.
    """
    profiles = get_all_profiles()
    if quiet:
        for profile in profiles:
            click.echo(profile[0])
    else:
        click.echo(tabulate(profiles, tablefmt='plain'))


@click.group(context_settings=CONTEXT_SETTINGS,
             short_help='Utility to to get info on CLI profiles.')
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
def profiles_group():
    """
    Utility to to get info on CLI profiles.
    """
    pass


profiles_group.add_command(list_cli, name='list')

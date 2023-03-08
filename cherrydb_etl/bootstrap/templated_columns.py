"""Creates columns from a template."""
import logging
from pathlib import Path

import click
import yaml
from cherrydb import CherryDB
from jinja2 import Template

from cherrydb_etl import TabularConfig, config_logger

log = logging.getLogger()


@click.command(
    context_settings={
        "ignore_unknown_options": True,
        "allow_extra_args": True,
    }
)
@click.option(
    "--template",
    "template_path",
    type=click.Path(exists=True, path_type=Path),
    required=True,
)
@click.option("--namespace", required=True)
@click.pass_context
def create_columns(ctx, template_path: Path, namespace: str):
    """Creates columns from a template."""
    # arbitrary argument parsing: https://stackoverflow.com/a/32946412
    template_args = {
        ctx.args[idx][2:]: ctx.args[idx + 1] for idx in range(0, len(ctx.args), 2)
    }
    with open(template_path) as config_fp:
        config_template = Template(config_fp.read())
    rendered_config = config_template.render(**template_args)
    config = TabularConfig(**yaml.safe_load(rendered_config))

    db = CherryDB(namespace=namespace)
    with db.context(
        notes=(
            f"ETL script {__name__}: creating columns from "
            f"template {template_path.parts[-1]}"
        )
    ) as ctx:
        for col in config.columns:
            log.info("Creating column %s in namespace %s...", col.target, namespace)
            ctx.columns.create(
                col.target,
                aliases=col.aliases,
                column_kind=col.kind,
                column_type=col.type,
                description=col.description,
                source_url=config.source_url,
            )


if __name__ == "__main__":
    config_logger(log)
    create_columns()

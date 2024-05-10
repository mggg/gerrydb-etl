"""Imports base Census geographies."""
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import click
import shapely.wkb
import yaml
from gerrydb import GerryDB
from gerrydb_etl import (TabularConfig, config_logger,
                         download_dataframe_with_hash)
from gerrydb_etl.bootstrap.pl_config import (AUXILIARY_LEVELS, LEVELS,
                                             MISSING_DATASETS, MissingDataset)
from jinja2 import Template
from shapely import Point

try:
    from gerrydb_etl.db import DirectTransactionContext
    from gerrydb_meta import crud, models, schemas
    from sqlalchemy import insert, select, update
except ImportError:
    crud = None

log = logging.getLogger()


SOURCE_URL = "https://www2.census.gov/geo/tiger/TIGER2020PL/"
LAYER_URLS = {
    "block/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/TABBLOCK/2010/tl_2020_{fips}_tabblock10.zip",
    "block/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/TABBLOCK/2020/tl_2020_{fips}_tabblock20.zip",
    "bg/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/BG/2010/tl_2020_{fips}_bg10.zip",
    "bg/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/BG/2020/tl_2020_{fips}_bg20.zip",
    "tract/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/TRACT/2010/tl_2020_{fips}_tract10.zip",
    "tract/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/TRACT/2020/tl_2020_{fips}_tract20.zip",
    "county/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/COUNTY/2010/tl_2020_{fips}_county10.zip",
    "county/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/COUNTY/2020/tl_2020_{fips}_county20.zip",
    "state/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/STATE/2010/tl_2020_{fips}_state10.zip",
    "state/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/STATE/2020/tl_2020_{fips}_state20.zip",
    "vtd/2010": "https://www2.census.gov/geo/tiger/TIGER2012/VTD/tl_2012_{fips}_vtd10.zip",
    "vtd/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/VTD/2020/tl_2020_{fips}_vtd20.zip",
    "place/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/PLACE/2010/tl_2020_{fips}_place10.zip",
    "place/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/PLACE/2020/tl_2020_{fips}_place20.zip",
    "cousub/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/COUSUB/2010/tl_2020_{fips}_cousub10.zip",
    "cousub/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/COUSUB/2020/tl_2020_{fips}_cousub20.zip",
    "aiannh/2010": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/AIANNH/2010/tl_2020_{fips}_aiannh10.zip",
    "aiannh/2020": "https://www2.census.gov/geo/tiger/TIGER2020PL/LAYER/AIANNH/2020/tl_2020_{fips}_aiannh20.zip",
}

COLUMN_CONFIG_PATH = Path(__file__).parent / "columns" / "pl_geo.yaml"


@click.command()
@click.option("--fips", help="State/territory FIPS code.", required=True)
@click.option("--level", type=click.Choice(LEVELS), required=True)
@click.option("--year", type=click.Choice(["2010", "2020"]), required=True)
@click.option("--namespace", required=True)
def load_geo(fips: str, level: str, year: str, namespace: str):
    """Imports base Census geographies.

    Preconditions:
        * A `Locality` aliased to `fips` exists.
        * `namespace` exists.
        * A `GeoLayer` with path `<level>/<year>` exists in the namespace.
    """
    if MissingDataset(fips=fips, level=level, year=year) in MISSING_DATASETS:
        log.warning("Dataset not published by Census. Nothing to do.")
        exit()

    if os.getenv("GERRYDB_BULK_IMPORT") and crud is None:
        raise RuntimeError("gerrydb_meta must be available in bulk import mode.")

    db = GerryDB(namespace=namespace)
    root_loc = db.localities[fips]
    layer = db.geo_layers[level]

    with open(COLUMN_CONFIG_PATH) as config_fp:
        config_template = Template(config_fp.read())
    rendered_config = config_template.render(yr=year[2:], year=year)
    config = TabularConfig(**yaml.safe_load(rendered_config))

    layer_url = LAYER_URLS[f"{level}/{year}"].format(fips=fips)
    index_col = "GEOID" + year[2:]
    county_col = "COUNTYFP" + year[2:]

    # to handle server side issues, try loading one more time if fail
    try:
        layer_gdf, layer_hash = download_dataframe_with_hash(
            url=layer_url,
            dtypes=config.source_dtypes(),
        )
    except:
        layer_gdf, layer_hash = download_dataframe_with_hash(
            url=layer_url,
            dtypes=config.source_dtypes(),
        )

    geos_by_county = (
        dict(layer_gdf.groupby(county_col)[index_col].apply(list))
        if county_col in layer_gdf.columns
        else {}
    )

    if level in AUXILIARY_LEVELS:
        # since aiannh geographies cross state lines, the census subidivides the polygon but
        # uses the same geoid, we add the fips code to make the geoid unique

        # remove the r,t that stands for reservation, trust which only appears 
        # at geo level, but not in pop data
        if level == "aiannh":
            def categorize_trust_res(x):
                if x[-1].lower() == "t":
                    return "trust"
                elif x[-1].lower() == "r":
                    return "reservation"
                else:
                    raise ValueError(f"Not a trust or reservation at geoid {x}")
                
            layer_gdf["res_trust_class"] = layer_gdf[index_col].apply(lambda x: categorize_trust_res(x))
            layer_gdf[index_col] = layer_gdf[index_col].apply(lambda x: f"{level}:" + x.rstrip("rtRT")+f":fips{fips}")
            yr = year[2:]

            # if there was a geoid with both an R and T tag
            if len(layer_gdf[index_col]) != len(set(layer_gdf[index_col])):
                new_rows = {}
                
                for row, data in layer_gdf.iterrows():
                    if data[index_col] not in new_rows:
                        new_rows[data[index_col]] = data
                        new_rows[data[index_col]]["collision_count"] = 0
                    
                    # if geoid already exists, indicates R/T collision
                    else:
                        new_rows[data[index_col]]["collision_count"] += 1
                        if new_rows[data[index_col]]["collision_count"] > 1:
                            raise ValueError(f"There has been a collision of 3 geoids {data[index_col]}")

                        # add land and water, change res/trust class, union geometry
                        new_rows[data[index_col]][f"ALAND{yr}"] += data[f"ALAND{yr}"]
                        new_rows[data[index_col]][f"AWATER{yr}"] += data[f"AWATER{yr}"]
                        new_rows[data[index_col]]["res_trust_class"] = "union"
                        new_rows[data[index_col]]["geometry"] = shapely.unary_union([new_rows[data[index_col]]["geometry"], data["geometry"]])

                        if new_rows[data[index_col]][f"NAME{yr}"] != data[f"NAME{yr}"]:
                            if data[index_col] in ["aiannh:1075:fips32", "aiannh:1070:fips32"]:
                                # the Fallon Paiute-Shoshone name has an extra (Reservation/Colony) appended
                                pass
                            else:
                                print("geoid", (data[index_col]))
                                print('name 1', new_rows[data[index_col]][f"NAME{yr}"])
                                print("name 2", data[f"NAME{yr}"])
                                raise ValueError(f"NAME{yr} does not match across R and T land in geoid {data[index_col]}")

                      
                layer_gdf = gpd.GeoDataFrame.from_dict(new_rows, orient = "index").set_crs(layer_gdf.crs)
            layer_gdf = layer_gdf[[f"NAME{yr}", index_col, "geometry", f"INTPTLAT{yr}", f"INTPTLON{yr}", "res_trust_class"]]
            

        else:
            layer_gdf[index_col] = f"{level}:" + layer_gdf[index_col]
        geos_by_county = {
            county: [f"{level}:{unit}" for unit in units]
            for county, units in geos_by_county.items()
        }
    layer_gdf = layer_gdf.set_index(index_col)

    
    columns = {
        col.source: db.columns[col.target]
        for col in config.columns
        if col.source in layer_gdf.columns
    }

    

    internal_latitudes = layer_gdf[f"INTPTLAT{year[2:]}"].apply(float)
    internal_longitudes = layer_gdf[f"INTPTLON{year[2:]}"].apply(float)
    layer_gdf["internal_point"] = [
        Point(long, lat) for long, lat in zip(internal_longitudes, internal_latitudes)
    ]

    import_notes = (
        f"Loaded by ETL script {__name__} from {year} U.S. Census {level} "
        f"shapefile {layer_url} (SHA256: {layer_hash.hexdigest()})"
    )

    if os.getenv("GERRYDB_BULK_IMPORT"):
        log.info(
            "Importing geographies via bulk import mode (direct database access)..."
        )
        with DirectTransactionContext(notes=import_notes) as ctx:
            now = datetime.now(timezone.utc)
            namespace_obj = crud.namespace.get(db=ctx.db, path=namespace)
            assert namespace_obj is not None

            # Create geographies (geolayers were created by pl_init, geolayers do not know about the actual geography objects yet) in bulk.
            log.info("Creating geographies...")
            geo_import, _ = crud.geo_import.create(
                db=ctx.db, obj_meta=ctx.meta, namespace=namespace_obj
            )
            geographies_raw = [
                schemas.GeographyCreate(
                    path=row.Index,
                    geography=shapely.wkb.dumps(row.geometry),
                    internal_point=shapely.wkb.dumps(row.internal_point),
                )
                for row in layer_gdf.itertuples()
            ]
            geographies, _ = crud.geography.create_bulk(
                db=ctx.db,
                objs_in=geographies_raw,
                obj_meta=ctx.meta,
                geo_import=geo_import,
                namespace=namespace_obj,
            )
            geos_by_path = {geo.path: geo for geo, _ in geographies}

            # Update column values in bulk.
            log.info("Updating column values...")
            raw_cols = (
                ctx.db.query(models.DataColumn)
                .filter(
                    models.DataColumn.col_id.in_(
                        select(models.ColumnRef.col_id).filter(
                            models.ColumnRef.path.in_(
                                col.canonical_path for col in columns.values()
                            ),
                            models.ColumnRef.namespace_id == namespace_obj.namespace_id,
                        )
                    )
                )
                .all()
            )
            cols_by_canonical_path = {col.canonical_ref.path: col for col in raw_cols}
            cols_by_alias = {
                alias: cols_by_canonical_path[col.canonical_path]
                for alias, col in columns.items()
            }
            ctx.load_column_values(cols=cols_by_alias, geos=geos_by_path, df=layer_gdf)

            # Create GeoSets (collections of geographys that instantiate a Locality) in bulk.
            log.info("Updating GeoSets...")
            layer_obj = crud.geo_layer.get(
                db=ctx.db, path=level, namespace=namespace_obj
            )
            full_fips = [fips] + [fips + county_fips for county_fips in geos_by_county]
            loc_ids = ctx.db.execute(
                select(models.Locality.loc_id, models.LocalityRef.path)
                .join(models.LocalityRef, onclause=models.Locality.refs)
                .filter(models.LocalityRef.path.in_(full_fips))
            )
            loc_ids_by_fips = {loc.path: loc.loc_id for loc in loc_ids}

            # ...but first, deprecate all the old ones.
            ctx.db.execute(
                update(models.GeoSetVersion)
                .where(
                    models.GeoSetVersion.layer_id == layer_obj.layer_id,
                    models.GeoSetVersion.loc_id.in_(loc.loc_id for loc in loc_ids),
                    models.GeoSetVersion.valid_to.is_(None),
                )
                .values(valid_to=now)
            )

            geo_sets = ctx.db.scalars(
                insert(models.GeoSetVersion).returning(models.GeoSetVersion),
                [
                    {
                        "layer_id": layer_obj.layer_id,
                        "loc_id": loc_id,
                        "valid_from": now,
                        "meta_id": ctx.meta.meta_id,
                    }
                    for loc_id in loc_ids_by_fips.values()
                ],
            )
            loc_id_to_set_id = {
                geo_set.loc_id: geo_set.set_version_id for geo_set in geo_sets
            }

            # Add members to GeoSets in bulk.
            log.info("Adding members to new GeoSets...")
            root_set_id = loc_id_to_set_id[loc_ids_by_fips[fips]]
            set_members = [
                {
                    "set_version_id": root_set_id,
                    "geo_id": geo.geo_id,
                }
                for geo, _ in geographies
            ]
            for county_fips, county_geos in geos_by_county.items():
                county_set_id = loc_id_to_set_id[loc_ids_by_fips[fips + county_fips]]
                for geo_path in county_geos:
                    set_members.append(
                        {
                            "set_version_id": county_set_id,
                            "geo_id": geos_by_path[geo_path].geo_id,
                        }
                    )
            ctx.db.execute(insert(models.GeoSetMember), set_members)
    else:
        log.info("Importing geographies via API...")
        with db.context(notes=import_notes) as ctx:
            
            ctx.load_dataframe(
                df=layer_gdf,
                columns=columns,
                create_geo=True,
                locality=root_loc,
                layer=layer,
            )
            for county_fips, county_geos in geos_by_county.items():
                full_fips = fips + county_fips
                ctx.geo_layers.map_locality(
                    layer=layer, locality=full_fips, geographies=county_geos
                )


if __name__ == "__main__":
    config_logger(log)
    load_geo()

import dagster as dg
from dagster_sling import SlingConnectionResource, SlingResource
from dagster import EnvVar, Nothing


club_shop_db_connection = SlingConnectionResource(
    name="CLUB_SHOP_DB",
    type="postgres",
    connection_string=EnvVar("CLUB_SHOP_DATABASE_URL"),
)


club_shop_replication_config = {
    "source": "CLUB_SHOP_DB",
    "target": "WAREHOUSE_DB",

    "defaults": {
        "mode": "full-refresh",
        "object": "club_shop.{stream_table}",
    },

    "streams": {
        "public.*": None,

        "public.__clubs_migrations": {"disabled": True},
        "public.sessions": {"disabled": True},

        # --- Exclude secrets ---
        "public.hcb_tokens": {
            "select": ["-access_token", "-refresh_token"],
        },
        "public.login_codes": {
            "select": ["-code_hash"],
        },
        "public.users": {
            "select": ["-hackatime_access_token"],
        },
    },
}



@dg.asset(
    name="club_shop_warehouse_mirror",
    group_name="sling",
    compute_kind="sling",
)
def club_shop_warehouse_mirror(
    context: dg.AssetExecutionContext,
    sling: SlingResource,
) -> Nothing:
    """Replicates the entire Club Shop DB → warehouse in a single shot."""
    context.log.info("Starting Club Shop → warehouse Sling replication")

    for _ in sling.replicate(
        context=context,
        replication_config=club_shop_replication_config,
    ):
        pass

    context.log.info("Replication finished")
    context.add_output_metadata({"replicated": True})
    return None

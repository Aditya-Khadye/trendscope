{% snapshot tickers_snapshot %}

{#
  SCD2 history of instrument metadata. The raw layer already appends a new
  version only when content changes; this snapshot adds dbt_valid_from /
  dbt_valid_to bookkeeping over the current view so marts can ask "what did
  we believe about this ticker on date X?". Benchmark is intentionally NOT
  snapshotted — it's transform-layer config (the seed), not upstream fact.
#}

{{
    config(
        unique_key='ticker',
        strategy='check',
        check_cols=['name', 'sector', 'industry', 'asset_type', 'groups'],
    )
}}

select
    ticker,
    name,
    sector,
    industry,
    asset_type,
    groups
from {{ ref('stg_tickers') }}

{% endsnapshot %}

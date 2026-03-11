# /// script
# [tool.marimo.display]
# theme = "dark"
# ///

import marimo

__generated_with = "0.20.4"
app = marimo.App(width="full")


@app.cell
def _():
    import os
    import marimo as mo

    return mo, os


@app.cell
def _():
    from quixlake import QuixLakeClient

    return (QuixLakeClient,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Query QuixLake Data
    """)
    return


@app.cell
def _(QuixLakeClient, os):
    # TODO: Replace with your QuixLake URL
    QUIXLAKE_URL = "https://quixlake-quixers-testrigdemodatawarehouse-prod.az-france-0.app.quix.io"

    client = QuixLakeClient(
        base_url=QUIXLAKE_URL,
        token=os.environ["QUIXLAKE_TOKEN"]
    )
    return (client,)


@app.cell
def _(mo):
    # TODO: Modify the SQL query for your data
    default_query = """
    SELECT
      DATE_TRUNC('minutes', to_timestamp(time/1000000000)) as "time_bucket",
      sum(abs("accelerometer-z")) as "acc_z",
      sum(abs("accelerometer-y")) as "acc_y",
      sum(abs("accelerometer-x")) as "acc_x",
      COUNT("accelerometer-z") as "count"
    FROM tomas
    WHERE "accelerometer-z" IS NOT NULL and time_bucket > '2026-03-11'
    GROUP BY time_bucket
    ORDER BY time_bucket
    LIMIT 100
    """.strip()

    sql_form = mo.ui.code_editor(
        value=default_query,
        language="sql",
        label="SQL query",
        min_height=150,
    ).form(submit_button_label="Run SQL")

    sql_form
    return (sql_form,)


@app.cell
def _(client, sql_form):
    df = client.query(sql_form.value)
    df
    return (df,)


@app.cell
def _(df, mo):
    import plotly.express as px
    fig = px.line(
        df,
        x="time_bucket",
        y="acc_x",
        title="Waveform",
    )
    mo.ui.plotly(fig)
    return (px,)


@app.cell
def _(df, px):
    import pandas as pd

    # Ensure proper data types
    df['time_bucket'] = pd.to_datetime(df['time_bucket'])
    df[['acc_x', 'acc_y', 'acc_z']] = df[['acc_x', 'acc_y', 'acc_z']].astype(float)

    # Reshape the data for grouped bar plot
    df_melted = pd.melt(
        df, 
        id_vars=['time_bucket'], 
        value_vars=['acc_x', 'acc_y', 'acc_z'],
        var_name='axis', 
        value_name='acceleration'
    )

    bar_fig = px.bar(
        df_melted,
        x="time_bucket",
        y="acceleration", 
        color="axis",
        title="Acceleration Data by Axis (Bar Chart)",
        labels={
            "acceleration": "Acceleration Value",
            "time_bucket": "Time",
            "axis": "Acceleration Axis"
        },
        color_discrete_map={
            "acc_x": "#FF6B6B",
            "acc_y": "#4ECDC4", 
            "acc_z": "#45B7D1"
        }
    )

    bar_fig.update_layout(
        xaxis_tickangle=-45,
        bargap=0.1
    )

    bar_fig
    return


if __name__ == "__main__":
    app.run()

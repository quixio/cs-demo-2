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
      DATE_TRUNC('days', to_timestamp(time/1000000000)) as "time_bucket",
      sum("accelerometer-z"),
      sum("accelerometer-y"),
      sum("accelerometer-x"),
      count("accelerometer-z") as "count"
    FROM ludvik
    WHERE  "accelerometer-z" is not NULL
    GROUP BY time_bucket
    LIMIT 1000
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
def _(df_accel, mo, px):
    fig_accel_bar = px.bar(
        df_accel,
        x="time_bucket",
        y="acceleration_sum",
        color="axis",
        title="Accelerometer Data (X, Y, Z) by Time Bucket",
        labels={
            "time_bucket": "Date",
            "acceleration_sum": "Acceleration Sum",
            "axis": "Accelerometer Axis"
        },
        barmode='group',
        color_discrete_map={
            'x': '#FF6B6B',
            'y': '#4ECDC4', 
            'z': '#45B7D1'
        }
    )

    fig_accel_bar.update_layout(
        xaxis_title="Date",
        yaxis_title="Acceleration Sum",
        legend_title="Axis"
    )

    mo.ui.plotly(fig_accel_bar)
    return


if __name__ == "__main__":
    app.run()

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
      DATE_TRUNC('hours', to_timestamp(time/1000000000)) as "time_bucket",
      acc_z as acc_z,
      acc_y as acc_y,
      acc_x as acc_x,
      count("accelerometer-z") as "count"
    FROM ludvik
    WHERE  "accelerometer-z" is not NULL AND time_bucket >'2026-03-11 9:00:00+00:00'
    GROUP BY time_bucket
    ORDER BY time_bucket
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
def _(df, mo, px):
    fig_bar_direct = px.bar(
        df,
        x="time_bucket",
        y=['acc_x', 'acc_y', 'acc_z'],
        title="Accelerometer Data (X, Y, Z) by Time Bucket",
        labels={
            "time_bucket": "Date",
            "value": "Acceleration Sum",
            "variable": "Accelerometer Axis"
        },
        barmode='group',
        color_discrete_map={
            'acc_x': '#FF6B6B',
            'acc_y': '#4ECDC4', 
            'acc_z': '#45B7D1'
        }
    )

    # Update legend labels to be cleaner
    fig_bar_direct.for_each_trace(lambda t: t.update(name = t.name.replace('sum("accelerometer-', 'acc_').replace('")', '')))

    fig_bar_direct.update_layout(
        xaxis_title="Date",
        yaxis_title="Acceleration Sum",
        legend_title="Accelerometer Axis"
    )

    mo.ui.plotly(fig_bar_direct)
    return


@app.cell
def _(df, mo, px):
    fig_bar_stacked = px.bar(
        df,
        x="time_bucket",
        y=['acc_x', 'acc_y', 'acc_z'],
        title="Accelerometer Data (X, Y, Z) Stacked by Time Bucket",
        labels={
            "time_bucket": "Date",
            "value": "Acceleration Sum",
            "variable": "Accelerometer Axis"
        },
        barmode='stack',
        color_discrete_map={
            'acc_x': '#FF6B6B',
            'acc_y': '#4ECDC4', 
            'acc_z': '#45B7D1'
        }
    )

    # Update legend labels to be cleaner
    fig_bar_stacked.for_each_trace(lambda t: t.update(name = t.name.replace('sum("accelerometer-', 'acc_').replace('")', '')))

    fig_bar_stacked.update_layout(
        xaxis_title="Date",
        yaxis_title="Acceleration Sum",
        legend_title="Accelerometer Axis"
    )

    mo.ui.plotly(fig_bar_stacked)
    return


if __name__ == "__main__":
    app.run()

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
      count("accelerometer-z") as "count"
    FROM tomas
    WHERE  "accelerometer-z" is not NULL AND time_bucket > '2026-03-11 09:00:00+00:00'
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
def _(df):
    import plotly.express as px

    # Reshape data for grouped bar chart with dark theme
    df_melted_dark = df.melt(
        id_vars=['time_bucket'], 
        value_vars=['acc_x', 'acc_y', 'acc_z'],
        var_name='accelerometer_axis',
        value_name='acceleration'
    )

    dark_bar_fig = px.bar(
        df_melted_dark,
        x="time_bucket",
        y="acceleration",
        color="accelerometer_axis",
        title="Accelerometer Data - Dark Theme Bar Chart",
        labels={
            "acceleration": "Acceleration Value",
            "time_bucket": "Time",
            "accelerometer_axis": "Axis"
        },
        color_discrete_map={
            "acc_x": "#FF6B6B",
            "acc_y": "#00D4AA", 
            "acc_z": "#64B5F6"
        },
        template="plotly_dark"
    )

    dark_bar_fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Acceleration Value",
        legend_title="Accelerometer Axis",
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white')
    )

    dark_bar_fig
    return


if __name__ == "__main__":
    app.run()

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
      sum(abs("accelerometer-z")) as acc_z,
      sum(abs("accelerometer-y")) as acc_y,
      sum(abs("accelerometer-x")) as acc_x,
      count("accelerometer-z") as "count"
    FROM ludvik
    WHERE  "accelerometer-z" is not NULL AND time_bucket >'2026-03-11 12:00:00+00:00' AND time_bucket <'2026-03-11 13:00:00+00:00'
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


@app.cell
def _():
    return


@app.cell
def _(mo):
    import datetime

    # Initialize default start and end dates
    default_start = datetime.datetime(2026, 3, 11, 12, 0, 0)
    default_end = datetime.datetime(2026, 3, 11, 13, 0, 0)

    # Create date and time picker controls for start date
    start_date_picker = mo.ui.date(
        value=default_start.date(),
        label="Start Date"
    )

    start_time_picker = mo.ui.text(
        value=default_start.strftime("%H:%M"),
        label="Start Time (HH:MM)"
    )

    # Create date and time picker controls for end date  
    end_date_picker = mo.ui.date(
        value=default_end.date(),
        label="End Date"
    )

    end_time_picker = mo.ui.text(
        value=default_end.strftime("%H:%M"),
        label="End Time (HH:MM)"
    )
    return (
        datetime,
        end_date_picker,
        end_time_picker,
        start_date_picker,
        start_time_picker,
    )


@app.cell
def _(
    end_date_picker,
    end_time_picker,
    mo,
    start_date_picker,
    start_time_picker,
):
    # Layout the date/time controls
    date_controls = mo.hstack([
        mo.vstack([
            mo.md("**Start Date & Time**"),
            start_date_picker,
            start_time_picker
        ], gap="0.5rem"),
        mo.vstack([
            mo.md("**End Date & Time**"),
            end_date_picker, 
            end_time_picker
        ], gap="0.5rem")
    ], justify="space-around", gap="2rem")

    date_controls
    return


@app.cell
def _(
    client,
    datetime,
    end_date_picker,
    end_time_picker,
    start_date_picker,
    start_time_picker,
):
    # Combine selected dates and times into datetime objects
    selected_start_datetime = datetime.datetime.combine(
        start_date_picker.value, 
        datetime.datetime.strptime(start_time_picker.value, "%H:%M").time()
    )

    selected_end_datetime = datetime.datetime.combine(
        end_date_picker.value,
        datetime.datetime.strptime(end_time_picker.value, "%H:%M").time()
    )

    # Generate SQL query based on selected date range
    custom_range_sql = f"""
    SELECT 
      DATE_TRUNC('minutes', to_timestamp(time/1000000000)) as "time_bucket",
      sum(abs("accelerometer-z")) as acc_z,
      sum(abs("accelerometer-y")) as acc_y,
      sum(abs("accelerometer-x")) as acc_x,
      count("accelerometer-z") as "count"
    FROM ludvik
    WHERE  "accelerometer-z" is not NULL 
      AND time_bucket >= '{selected_start_datetime.strftime('%Y-%m-%d %H:%M:%S')}+00:00'
      AND time_bucket < '{selected_end_datetime.strftime('%Y-%m-%d %H:%M:%S')}+00:00'
    GROUP BY time_bucket
    ORDER BY time_bucket
    LIMIT 1000
    """.strip()

    # Query data for selected time range
    custom_df = client.query(custom_range_sql)
    custom_df
    return custom_df, selected_end_datetime, selected_start_datetime


@app.cell
def _(custom_df, mo, px, selected_end_datetime, selected_start_datetime):
    # Calculate duration for display
    duration = selected_end_datetime - selected_start_datetime
    duration_hours = duration.total_seconds() / 3600

    # Create plot with custom date range
    custom_fig = px.bar(
        custom_df,
        x="time_bucket",
        y=['acc_x', 'acc_y', 'acc_z'],
        title=f"Accelerometer Data - Custom Range ({duration_hours:.1f} hours)",
        labels={
            "time_bucket": "Time",
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

    custom_fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Acceleration Sum",
        legend_title="Accelerometer Axis",
        height=500,
        margin=dict(l=60, r=60, t=60, b=60)
    )

    mo.ui.plotly(custom_fig)
    return duration, duration_hours


@app.cell
def _(
    custom_df,
    duration,
    duration_hours,
    mo,
    selected_end_datetime,
    selected_start_datetime,
):
    mo.md(f"""
    ### Custom Date Range Analysis

    **Selected Time Range:** {selected_start_datetime.strftime('%Y-%m-%d %H:%M:%S')} to {selected_end_datetime.strftime('%Y-%m-%d %H:%M:%S')} UTC

    **Duration:** {duration_hours:.1f} hours ({int(duration.total_seconds() / 60)} minutes)

    **Data Points:** {len(custom_df)} time buckets

    Use the date and time pickers above to select any custom time range for analysis. The plot will automatically update to show accelerometer data (X, Y, Z axes) for your selected period.
    """)
    return


if __name__ == "__main__":
    app.run()

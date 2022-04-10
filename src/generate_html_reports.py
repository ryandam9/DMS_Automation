import os
from datetime import datetime


def generate_table_metadata_compare_report(df, src_db_config, tgt_db_config):
    """ 
    Generates a HTML Report for Table Metadata comparison.

    :param df: Dataframe containing the table metadata comparison.
    :param src_db_config: Source database configuration.
    :param tgt_db_config: Target database configuration.
    """
    html_table_data = ""

    for row in df.values.tolist():
        html_row = "<tr>"

        for cell in row:
            html_row += f"<td>{cell}</td>"

        html_row += "</tr>"
        html_table_data += html_row

    html_template = ""

    # Read HTML template for this report.
    with open("../config/html/layout_1.html") as template:
        html_template = template.read()

    # Replace the table data in the HTML template.
    html_template = html_template.replace(
        "generate_table_metadata_compare_report", html_table_data
    )

    html_template = (
        html_template.replace("[src_db_engine]", src_db_config["db_engine"])
        .replace("[src_host]", src_db_config["host"])
        .replace("[src_port]", str(src_db_config["port"]))
        .replace("[src_database]", src_db_config["service"])
        .replace("[src_user]", src_db_config["user"])
        .replace("[tgt_db_engine]", tgt_db_config["db_engine"])
        .replace("[tgt_host]", tgt_db_config["host"])
        .replace("[tgt_port]", str(tgt_db_config["port"]))
        .replace("[tgt_database]", tgt_db_config["service"])
        .replace("[tgt_user]", tgt_db_config["user"])
    )

    current_time = (
        datetime.now().strftime("%Y_%m_%d %H:%M").replace(" ", "_").replace(":", "_")
    )

    # Write the HTML report to a file.
    html_report = f"../table_structure_validation/table_metadata_compare_report_{current_time}.html"

    with open(html_report, "w") as report:
        report.write(html_template)

    print(f"-> HTML report generated: {os.path.abspath(html_report)}")


def generate_data_validation_report(summary_rows, col_differences, counts):
    """
    Generates a HTML report for Data validation.

    :param data: A list of lists.

    """
    html_summary_table_data = ""

    for row in summary_rows:
        try:
            (
                schema,
                table,
                no_records_validated,
                no_records_differences,
                columns,
                msg,
            ) = row[0], row[1], row[2], row[3], row[4], row[5]
        except Exception as err:
            print(row)
            continue

        html_row = "<tr>"
        html_row += f"<td>{schema}</td>"
        html_row += f"<td>{table}</td>"
        html_row += f"<td>{no_records_validated}</td>"
        html_row += f"<td>{no_records_differences}</td>"

        if len(columns.strip()) > 0:
            ul = "<ul>"
            for col in columns.split(","):
                ul += f"<li>{col}</li>"
            ul += "</ul>"

            html_row += f"<td>{ul}</td>"
        else:
            html_row += f"<td></td>"

        # Append message.
        if "no data differences found" in msg.lower():
            html_row += f"<td class='bg-success text-light'>{msg}</td>"
        elif "no data found" in msg.lower():
            html_row += f"<td class='bg-info'>{msg}</td>"
        else:
            html_row += f"<td class='bg-warning'>{msg}</td>"

        html_row += "</tr>"
        html_summary_table_data += html_row

    # Now, process Column level differences.
    html_col_diff_data = ""

    for row in col_differences:
        html_col_diff_data += "<tr>"

        for cell in row:
            html_col_diff_data += f"<td>{cell}</td>"

        html_col_diff_data += "</tr>"

    # Read HTML template for this report.
    html_template = ""

    with open("../config/html/layout_2.html") as template:
        html_template = template.read()

    # Replace the table data in the HTML template.
    html_template = html_template.replace(
        "summary_placeholder_data", html_summary_table_data)
    html_template = html_template.replace(
        "column_differences_placeholder_data", html_col_diff_data)

    html_template = html_template.replace(
        "total_tables_validated_count", str(counts['total_tables']))

    html_template = html_template.replace(
        "complete_match_tables_count", str(counts['complete_match_tables']))

    html_template = html_template.replace(
        "tables_with_differences_count", str(counts['tables_with_differences']))

    html_template = html_template.replace(
        "skip_tables_count", str(counts['skip_tables']))

    html_template = html_template.replace(
        "error_tables_count", str(counts['error_tables']))

    current_time = (
        datetime.now().strftime("%Y_%m_%d %H:%M").replace(" ", "_").replace(":", "_")
    )

    # Write the HTML report to a file.
    html_report = f"../data_validation/data_validation_report_{current_time}.html"

    with open(html_report, "w") as report:
        report.write(html_template)

    print(f"-> HTML report generated: {os.path.abspath(html_report)}")

"""
Fetches last 10 days of data from some important tables, calculates some
statistics, formats the results as an ASCII-table and sends this result to slack.
"""


from google.cloud import bigquery
import json
import requests
import os
from datetime import date,timedelta
from typing import List
import pandas as pd


from prepare_data_query import prepare_data_query


slack_webhook = os.environ.get("SLACK_WEBHOOK")


def print_log(message:str, severity:str = "INFO") -> None:
    entry = dict(
        severity=severity,
        message=message
    )

    print(json.dumps(entry))


def query_gbq(prepared_query:str = prepare_data_query) -> pd.DataFrame:
    print_log("Querying BigQuery")

    client = bigquery.Client()
    query_job = client.query(prepare_data_query.prepare_data_query)
    rows = query_job.result()
    
    return rows.to_dataframe()


def create_slack_table(df:pd.DataFrame) -> List[str]:

    row_text = ""
               
    for ind,row in df.iterrows():
        print_values = []
        at_least_one_number = False

        for val in row.values:
            if isinstance(val, str):
                print_values.append(val)
            elif pd.isna(val):
                print_values.append("█")
            elif isinstance(val, float):
                at_least_one_number = True
                if val > 1e6:
                    print_values.append('{:,.1f} M'.format(val/1e6))
                elif 0 < abs(val) < 1:
                    print_values.append('{:.2f}'.format(val))
                else:
                    print_values.append(str(int(val)))
            else:
                print_values.append(val)
        
        if at_least_one_number:
            row_text += "│ {:>19.19} │ {:<29.29} │ {:>9} │ {:>7.7} │ {:>7.7} │ {:>7.7} │ {:>7.7} │ {:>7.7} │ {:>7.7} │ {:>7.7} │ {:>7.7} │ {:>7.7} │ {:>7.7} │".format(*(print_values)) + "\n"
        
        
    num_chunks = len(row_text)//3000 +1
    num_lines = row_text.count("\n")
    lines_per_chunk = num_lines//num_chunks +1

    # This part can be simplified significantly using the 'tabulate' package, if your data is uniform in format
    row_text = row_text.splitlines(True)

    col_headers = "│           dimension │ table                         │       kpi │  "
    col_headers += "  │  ".join([(date.today() + timedelta(days=-10) + timedelta(days=i)).strftime("%m-%d") for i in range(10)])
    col_headers += "  |"

    table_header ="```"+ "┌" +"─"* (len(col_headers)-2) + "┐"+ "\n"
    table_header += col_headers + "\n"
    table_header += "├" + "─"*(len(col_headers)-2) + "┤"+ "\n"
     
    table_footer = "└" + "─"*(len(col_headers)-2) + "┘" 
    table_footer += "```"

    result_list = []

    for chunk in range(num_chunks):
        table_chunk = table_header

        for line in range(chunk*lines_per_chunk, (chunk+1)*lines_per_chunk):
            try:
                table_chunk += row_text[line]
            except IndexError:
                pass

        table_chunk += table_footer
        result_list.append(table_chunk)

    return result_list

def send_to_slack_text(preformatted_text:str) -> None:

    blocks = {
    			"text": preformatted_text
             }
    
    data = json.dumps(blocks)

    headers = {
        'Content-Type': 'application/json',
        'Content-Length': str(len(data))
    }

    requests.post(slack_webhook, data=data, headers=headers)

    return None


def send_to_slack_button() -> None:

    today = date.today().strftime("%Y-%m-%d")
    
    blocks = {
      	"blocks": [
      		{
      			"type": "context",
      			"elements": [
      				{
      					"type": "mrkdwn",
      					"text": f"*:ticket:* <http://gitlab.com/organization/group/project/-/issues/new?issue[title]=Data%20{today}&issuable_template=Incident%20Data&issue[issue_type]=incident|create issue.>"
      				}
      			]
      		}
      	]
      }

    data = json.dumps(blocks)

    headers = {
        'Content-Type': 'application/json',
        'Content-Length': str(len(data))
    }


    result = requests.post(slack_webhook, data=data, headers=headers)


def main(ctx):

    for table in create_slack_table(query_gbq()):
        send_to_slack_text(table)
    send_to_slack_button()

    return "Finished!"

if __name__ == "__main__":
    main({})